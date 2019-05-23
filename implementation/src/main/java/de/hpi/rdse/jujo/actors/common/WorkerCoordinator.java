package de.hpi.rdse.jujo.actors.common;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.util.ByteString;
import de.hpi.rdse.jujo.actors.slave.CorpusReceiver;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public class WorkerCoordinator extends AbstractReapedActor {

    public static Props props(String tempWorkingDir) {
        return Props.create(WorkerCoordinator.class, () -> new WorkerCoordinator(tempWorkingDir));
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
    private ActorRef wordCountWorker;
    private final ActorRef corpusReceiver;

    private WorkerCoordinator(String tempWorkingDir) {
        this.wordEndpoint = this.context().actorOf(WordEndpoint.props(), WordEndpoint.DEFAULT_NAME);
        this.corpusReceiver = this.context().actorOf(
                CorpusReceiver.props(this.self(), tempWorkingDir));
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
        initializeWordCountWorker();
        this.wordCountWorker.tell(message, this.self());
    }

    private void handle(CorpusTransferCompleted message) {
        initializeWordCountWorker();
        this.wordCountWorker.tell(message, this.self());
    }

    private void initializeWordCountWorker() {
        if (this.wordCountWorker == null) {
            this.wordCountWorker = this.context().actorOf(WordCountWorker.props(this.wordEndpoint));
        }
    }

}
