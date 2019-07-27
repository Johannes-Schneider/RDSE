package de.hpi.rdse.jujo.actors.common.training;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.RootActorPath;
import akka.actor.Terminated;
import akka.routing.ActorRefRoutee;
import akka.routing.Broadcast;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.common.Subsampler;
import de.hpi.rdse.jujo.actors.common.WordEndpoint;
import de.hpi.rdse.jujo.wordManagement.WordEndpointResolver;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

public class TrainingCoordinator extends AbstractReapedActor {

    public static Props props(int numberOfLocalWorkers) {
        return Props.create(TrainingCoordinator.class, () -> new TrainingCoordinator(numberOfLocalWorkers));
    }

    @NoArgsConstructor @AllArgsConstructor @Builder @Getter
    public static class StartTraining implements Serializable {
        private static final long serialVersionUID = -910991812790625629L;
        private int numberOfLocalWorkers;
        private String localCorpusPartitionPath;
    }

    @NoArgsConstructor
    public static class SkipGramsDistributed implements Serializable {
        private static final long serialVersionUID = 4150057273673434932L;
    }

    @NoArgsConstructor @AllArgsConstructor @Getter
    public static class SkipGramChunkTransferred implements Serializable {
        private static final long serialVersionUID = -3803848151388038254L;
        private ActorRef producer;
        private ActorRef consumer;
    }

    @NoArgsConstructor @AllArgsConstructor @Getter
    public static class EndOfTraining implements Serializable {
        private static final long serialVersionUID = -8558419590883496767L;
        private ActorRef producer;
    }

    private ActorRef skipGramDistributor;
    private ActorRef resultPartitionSender;
    private boolean trainingHasStarted = false;
    private boolean isTrainingFinished = false;
    private Router skipGramReceiverRouter;
    private final int numberOfLocalWorkers;
    private final Queue<SkipGramReceiver.ProcessEncodedSkipGram> trainingBuffer = new LinkedList<>();
    private final Set<RootActorPath> activeSkipGramProducers = new HashSet<>();


    private TrainingCoordinator(int numberOfLocalWorkers) {
        this.numberOfLocalWorkers = numberOfLocalWorkers;
        for (ActorRef endpoint : WordEndpointResolver.getInstance().all()) {
            this.activeSkipGramProducers.add(endpoint.path().root());
        }
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(StartTraining.class, this::handle)
                   .match(SkipGramsDistributed.class, this::handle)
                   .match(SkipGramReceiver.ProcessEncodedSkipGram.class, this::handle)
                   .match(SkipGramChunkTransferred.class, this::handle)
                   .match(Terminated.class, this::handle)
                   .match(EndOfTraining.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private void handle(StartTraining message) {
        this.log().info("Start training!");
        this.skipGramReceiverRouter = this.createRoundRobinRouter(this.numberOfLocalWorkers);
        this.initializeAndStartSkipGramDistribution(message.getLocalCorpusPartitionPath());
        this.trainingHasStarted = true;
        this.processTrainingBuffer();
    }

    private void processTrainingBuffer() {
        while (!this.trainingBuffer.isEmpty()) {
            this.skipGramReceiverRouter.route(this.trainingBuffer.poll(), this.self());
        }
    }

    private Router createRoundRobinRouter(int numberOfWorkers) {
        List<Routee> workers = new ArrayList<>();
        for (int i = 0; i < numberOfWorkers; i++) {
            workers.add(this.createWorker());
        }
        return new Router(new RoundRobinRoutingLogic(), workers);
    }

    private ActorRefRoutee createWorker() {
        ActorRef worker = this.context().actorOf(SkipGramReceiver.props());
        this.context().watch(worker);
        return new ActorRefRoutee(worker);
    }

    private void initializeAndStartSkipGramDistribution(String localCorpusPartitionPath) {
        if (this.skipGramDistributor != null) {
            return;
        }
        this.skipGramDistributor = this.context().actorOf(SkipGramDistributor.props(localCorpusPartitionPath));
    }

    private void handle(SkipGramsDistributed message) {
        this.log().info("Successfully distributed all skip-grams");
        this.sender().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    private void handle(SkipGramReceiver.ProcessEncodedSkipGram message) {
        if (!this.trainingHasStarted) {
            this.log().debug(String.format("Buffering encoded skip-gram \"%s\" from %s",
                    message.getSkipGram().getExpectedOutput(), this.sender().path()));

            this.trainingBuffer.add(message);
            return;
        }

        this.log().debug(String.format("Processing encoded skip-gram (expected output = \"%s\") from %s",
                message.getSkipGram().getExpectedOutput(), this.sender().path()));

        this.skipGramReceiverRouter.route(message, this.sender());
    }

    private void handle(SkipGramChunkTransferred message) {
        this.log().info(String.format("Requesting next skip gram batch from %s", message.getProducer().path()));
        message.getProducer().tell(new SkipGramDistributor.RequestNextSkipGramChunk(), this.self());
    }

    private void handle(Terminated message) {
        this.context().unwatch(message.actor());

        if (message.actor() == this.resultPartitionSender) {
            this.log().info("Result transfer completed. About to unsubscribe from WordEndpoints.");
            WordEndpointResolver.getInstance().broadcast(new WordEndpoint.Unsubscribe(), this.self());
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
            return;
        }

        if (!this.skipGramReceiverRouter.routees().contains(message.actor())) {
            return;
        }

        this.log().info("Skip gram receiver terminated");
        this.skipGramReceiverRouter = this.skipGramReceiverRouter.removeRoutee(message.actor());
        if (!this.isTrainingFinished) {
            this.skipGramReceiverRouter.addRoutee(this.createWorker());
            return;
        }
        if (!this.skipGramReceiverRouter.routees().isEmpty()) {
            return;
        }
        this.initializeResultTransfer();
    }

    private void initializeResultTransfer() {
        this.resultPartitionSender = this.context().actorOf(ResultPartitionSender.props());
        this.context().watch(this.resultPartitionSender);
    }

    private void handle(EndOfTraining message) {
        this.activeSkipGramProducers.remove(message.getProducer().path().root());
        this.log().info(String.format("End of training received from %s. Waiting for %d more producers to finish",
                message.getProducer().path().root(), this.activeSkipGramProducers.size()));

        if (this.activeSkipGramProducers.isEmpty()) {
            this.finalizeTraining();
        }
    }

    private void finalizeTraining() {
        this.isTrainingFinished = true;
        this.skipGramReceiverRouter.route(new Broadcast(PoisonPill.getInstance()), ActorRef.noSender());
    }
}
