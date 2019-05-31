package de.hpi.rdse.jujo.actors.common;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.util.ByteString;
import de.hpi.rdse.jujo.actors.common.wordCount.WordCountCoordinator;
import de.hpi.rdse.jujo.actors.slave.CorpusReceiver;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public class WorkerCoordinator extends AbstractReapedActor {

    public static Props props(String tempWorkingDir, int maxNumberOfLocalWorkers) {
        return Props.create(WorkerCoordinator.class,
                () -> new WorkerCoordinator(tempWorkingDir, maxNumberOfLocalWorkers));
    }

    @AllArgsConstructor @NoArgsConstructor @Getter
    public static class ProcessCorpusChunk implements Serializable {
        private static final long serialVersionUID = 8963183281702412326L;
        private ByteString corpusChunk;
    }

    @NoArgsConstructor
    public static class CorpusTransferCompleted implements Serializable {
        private static final long serialVersionUID = 425628626453556436L;
    }

    private final ActorRef wordEndpoint;
    private ActorRef wordCountCoordinator;
    private final ActorRef corpusReceiver;
    private final int maxNumberOfLocalWorkers;

    private WorkerCoordinator(String tempWorkingDir, int maxNumberOfLocalWorkers) {
        this.wordEndpoint = this.context().actorOf(WordEndpoint.props(), WordEndpoint.DEFAULT_NAME);
        this.corpusReceiver = this.context().actorOf(
                CorpusReceiver.props(this.self(), tempWorkingDir));
        this.maxNumberOfLocalWorkers = maxNumberOfLocalWorkers;
    }


    @Override public Receive createReceive() {
        return defaultReceiveBuilder()
                .match(CorpusReceiver.ProcessCorpusPartition.class, this::handle)
                .match(ProcessCorpusChunk.class, this::handle)
                .match(CorpusTransferCompleted.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(CorpusReceiver.ProcessCorpusPartition message) {
        this.corpusReceiver.tell(message, this.sender());
    }

    private void handle(ProcessCorpusChunk message) {
        initializeWordCountCoordinator();
        this.wordCountCoordinator.tell(message, this.self());
    }

    private void handle(CorpusTransferCompleted message) {
        initializeWordCountCoordinator();
        this.wordCountCoordinator.tell(message, this.self());
    }

    private void initializeWordCountCoordinator() {
        if (this.wordCountCoordinator == null) {
            this.wordCountCoordinator = this.context().actorOf(
                    WordCountCoordinator.props(this.wordEndpoint, this.maxNumberOfLocalWorkers));
        }
    }

}
