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
import de.hpi.rdse.jujo.actors.common.WordEndpoint;
import de.hpi.rdse.jujo.utils.Utility;
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
    private final ActorRef resultPartitionSender;
    private boolean trainingHasStarted = false;
    private Router skipGramReceiverRouter;
    private final Queue<SkipGramReceiver.ProcessEncodedSkipGram> trainingBuffer = new LinkedList<>();
    private final Set<RootActorPath> activeSkipGramProducers = new HashSet<>();


    private TrainingCoordinator(int numberOfLocalWorkers) {
        for (ActorRef endpoint : WordEndpointResolver.getInstance().all()) {
            this.activeSkipGramProducers.add(endpoint.path().root());
        }
        this.skipGramReceiverRouter = this.createSkipGramReceiverRouter(numberOfLocalWorkers);
        this.resultPartitionSender = this.spawnChild(ResultPartitionSender.props());
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(StartTraining.class, this::handle)
                   .match(SkipGramReceiver.ProcessEncodedSkipGram.class, this::handle)
                   .match(SkipGramChunkTransferred.class, this::handle)
                   .match(EndOfTraining.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private Router createSkipGramReceiverRouter(int numberOfWorkers) {
        List<Routee> skipGramReceivers = new ArrayList<>();
        for (int i = 0; i < numberOfWorkers; i++) {
            skipGramReceivers.add(this.createSkipGramReceiver());
        }
        return new Router(new RoundRobinRoutingLogic(), skipGramReceivers);
    }

    private ActorRefRoutee createSkipGramReceiver() {
        ActorRef skipGramReceiver = this.spawnChild(SkipGramReceiver.props());
        return new ActorRefRoutee(skipGramReceiver);
    }

    private void handle(StartTraining message) {
        this.logProcessStep("Start Training");

        this.trainingHasStarted = true;
        this.initializeAndStartSkipGramDistribution(message.getLocalCorpusPartitionPath());
        this.processTrainingBuffer();
    }

    private void initializeAndStartSkipGramDistribution(String localCorpusPartitionPath) {
        if (this.skipGramDistributor != null) {
            return;
        }
        this.skipGramDistributor = this.spawnChild(SkipGramDistributor.props(localCorpusPartitionPath));
    }

    private void processTrainingBuffer() {
        while (!this.trainingBuffer.isEmpty()) {
            this.skipGramReceiverRouter.route(this.trainingBuffer.poll(), this.self());
        }
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

    @Override
    protected void handleTerminated(Terminated message) {
        super.handleTerminated(message);

        if (Utility.isRoutee(this.skipGramReceiverRouter, message.actor())) {
            this.skipGramReceiverRouter = this.skipGramReceiverRouter.removeRoutee(message.actor());

            if (!this.isPurposeFulfilled()) {
                this.respawnSkipGramReceiver();
                return;
            }

            this.log().info("Skip gram receiver terminated");
            if (!this.skipGramReceiverRouter.routees().isEmpty()) {
                // wait for all other skip gram receivers to terminate
                return;
            }

            this.unsubscribeFromAllWordEndpoints();
            this.initializeResultTransfer();
        }
    }

    private void unsubscribeFromAllWordEndpoints() {
        WordEndpointResolver.getInstance().broadcast(new WordEndpoint.Unsubscribe(), this.self());
    }

    private void respawnSkipGramReceiver() {
        this.skipGramReceiverRouter.addRoutee(this.createSkipGramReceiver());
    }

    private void initializeResultTransfer() {
        this.resultPartitionSender.tell(new ResultPartitionSender.StartTransfer(), this.self());
    }

    private void handle(EndOfTraining message) {
        this.activeSkipGramProducers.remove(message.getProducer().path().root());
        this.log().info(String.format("End of training received from %s. Waiting for %d more producers to finish",
                                      message.getProducer().path().root(), this.activeSkipGramProducers.size()));

        if (this.activeSkipGramProducers.isEmpty()) {
            this.purposeHasBeenFulfilled();
            this.terminateAllSkipGramReceivers();
        }
    }

    private void terminateAllSkipGramReceivers() {
        this.skipGramReceiverRouter.route(new Broadcast(PoisonPill.getInstance()), ActorRef.noSender());
    }
}
