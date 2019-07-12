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
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WordCountCoordinator extends AbstractReapedActor {

    public static Props props(ActorRef supervisor, int maxNumberOfLocalWorkers) {
        return Props.create(WordCountCoordinator.class, () -> new WordCountCoordinator(supervisor, maxNumberOfLocalWorkers));
    }

    @NoArgsConstructor @AllArgsConstructor @Getter
    public static class WordsCounted implements Serializable {
        private static final long serialVersionUID = 6986487041285352083L;
        private Map<String, Integer> wordCounts;
    }

    private final ActorRef supervisor;
    private final Map<String, Long> wordCounts = new HashMap<>();
    private Router router;
    private boolean wordCountFinished = false;
    private final CorpusReassembler corpusReassembler;

    private WordCountCoordinator(ActorRef supervisor, int maxNumberOfLocalWorkers) {
        this.supervisor = supervisor;
        this.router = this.createRoundRobinRouter(maxNumberOfLocalWorkers);
        this.corpusReassembler = new CorpusReassembler(FilePartitionIterator.chunkSize());
    }

    private Router createRoundRobinRouter(int numberOfWorkers) {
        List<Routee> workers = new ArrayList<>();
        for (int i = 0; i < numberOfWorkers; i++) {
            workers.add(this.createWorker());
        }
        return new Router(new RoundRobinRoutingLogic(), workers);
    }

    private ActorRefRoutee createWorker() {
        ActorRef worker = this.context().actorOf(WordCountWorker.props());
        this.context().watch(worker);
        return new ActorRefRoutee(worker);
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                .match(WorkerCoordinator.ProcessCorpusChunk.class, this::handle)
                .match(WorkerCoordinator.CorpusTransferCompleted.class, this::handle)
                .match(WordsCounted.class, this::handle)
                .match(Terminated.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(WorkerCoordinator.ProcessCorpusChunk message) {
        if (this.wordCountFinished) {
            this.log().error("Received CorpusChunk to process although transfer already finished");
        }
        String decodedChunk = this.corpusReassembler.decodeCorpusUntilNextDelimiter(message.getCorpusChunk());
        this.router.route(new WordCountWorker.CountWords(decodedChunk), this.self());
    }

    private void handle(WorkerCoordinator.CorpusTransferCompleted message) {
        String decodedChunk = this.corpusReassembler.decodeRemainingCorpus();
        this.router.route(new WordCountWorker.CountWords(decodedChunk), this.self());
        this.wordCountFinished = true;
        this.router.route(new Broadcast(PoisonPill.getInstance()), ActorRef.noSender());
    }

    private void handle(WordsCounted message) {
        for (Map.Entry<String, Integer> wordCount : message.getWordCounts().entrySet()) {
            if (this.wordCounts.computeIfPresent(wordCount.getKey(), (key, value) -> value + wordCount.getValue()) == null) {
                this.wordCounts.put(wordCount.getKey(), (long) wordCount.getValue());
            }
        }
    }

    private void handle(Terminated message) {
        this.router = this.router.removeRoutee(message.actor());
        if (this.wordCountFinished) {
            if (this.router.routees().size() < 1) {
                this.supervisor.tell(new Subsampler.WordsCounted(this.wordCounts), this.self());
            }
            return;
        }
        this.router.addRoutee(this.createWorker());
    }
}
