package de.hpi.rdse.jujo.actors.common.wordCount;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.routing.ActorRefRoutee;
import akka.routing.Broadcast;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.common.Subsampler;
import de.hpi.rdse.jujo.actors.common.WorkerCoordinator;
import de.hpi.rdse.jujo.corpusHandling.CorpusReassembler;
import de.hpi.rdse.jujo.fileHandling.FilePartitionIterator;
import de.hpi.rdse.jujo.utils.Utility;
import de.hpi.rdse.jujo.wordManagement.WordEndpointResolver;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class WordCountCoordinator extends AbstractReapedActor {

    public static Props props(int maxNumberOfLocalWorkers) {
        return Props.create(WordCountCoordinator.class, () -> new WordCountCoordinator(maxNumberOfLocalWorkers));
    }

    @NoArgsConstructor @AllArgsConstructor @Getter
    public static class WordsCounted implements Serializable {
        private static final long serialVersionUID = 6986487041285352083L;
        private Map<String, Integer> wordCounts;
    }

    private final Map<String, Long> wordCounts = new HashMap<>();
    private Router wordCountRouter;
    private final CorpusReassembler corpusReassembler;

    private WordCountCoordinator(int maxNumberOfLocalWorkers) {
        this.wordCountRouter = this.createWordCountRouter(maxNumberOfLocalWorkers);
        this.corpusReassembler = new CorpusReassembler(FilePartitionIterator.chunkSize());
    }

    private Router createWordCountRouter(int numberOfWorkers) {
        List<Routee> workers = new ArrayList<>();
        for (int i = 0; i < numberOfWorkers; i++) {
            workers.add(this.createWordCountWorker());
        }
        return new Router(new RoundRobinRoutingLogic(), workers);
    }

    private ActorRefRoutee createWordCountWorker() {
        ActorRef worker = this.spawnChild(WordCountWorker.props());
        return new ActorRefRoutee(worker);
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(WorkerCoordinator.ProcessCorpusChunk.class, this::handle)
                   .match(WorkerCoordinator.CorpusTransferCompleted.class, this::handle)
                   .match(WordsCounted.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private void handle(WorkerCoordinator.ProcessCorpusChunk message) {
        if (this.isPurposeFulfilled()) {
            this.log().error("Received CorpusChunk to process although transfer already finished");
        }
        String decodedChunk = this.corpusReassembler.decodeCorpusUntilNextDelimiter(message.getCorpusChunk());
        this.wordCountRouter.route(new WordCountWorker.CountWords(decodedChunk), this.self());
    }

    private void handle(WorkerCoordinator.CorpusTransferCompleted message) {
        String decodedChunk = this.corpusReassembler.decodeRemainingCorpus();
        this.wordCountRouter.route(new WordCountWorker.CountWords(decodedChunk), this.self());
        this.purposeHasBeenFulfilled();
        this.wordCountRouter.route(new Broadcast(PoisonPill.getInstance()), ActorRef.noSender());
    }

    private void handle(WordsCounted message) {
        for (Map.Entry<String, Integer> wordCount : message.getWordCounts().entrySet()) {
            if (this.wordCounts.computeIfPresent(wordCount.getKey(), (key, value) -> value + wordCount.getValue()) == null) {
                this.wordCounts.put(wordCount.getKey(), (long) wordCount.getValue());
            }
        }
    }

    @Override
    protected void handleTerminated(Terminated message) {
        super.handleTerminated(message);

        if (!Utility.isRoutee(this.wordCountRouter, message.actor())) {
            return;
        }

        this.wordCountRouter = this.wordCountRouter.removeRoutee(message.actor());

        if (!this.isPurposeFulfilled()) {
            this.respawnWordCountWorker();
            return;
        }

        if (this.wordCountRouter.routees().size() > 0) {
            // wait for all word count workers to be terminated
            return;
        }

        ActorRef localWordEndpoint = WordEndpointResolver.getInstance().localWordEndpoint();
        localWordEndpoint.tell(new Subsampler.WordsCounted(this.wordCounts), this.self());
    }

    private void respawnWordCountWorker() {
        this.wordCountRouter.addRoutee(this.createWordCountWorker());
    }
}
