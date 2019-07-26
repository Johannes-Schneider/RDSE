package de.hpi.rdse.jujo.actors.common;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.util.ByteString;
import de.hpi.rdse.jujo.actors.common.training.SkipGramReceiver;
import de.hpi.rdse.jujo.actors.common.training.TrainingCoordinator;
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

    @NoArgsConstructor @AllArgsConstructor @Getter
    public static class CorpusTransferCompleted implements Serializable {
        private static final long serialVersionUID = 425628626453556436L;
        private String localCorpusPartitionPath;
    }

    @NoArgsConstructor
    public static class VocabularyReadyForTraining implements Serializable {
        private static final long serialVersionUID = -880424032423441020L;
    }


    private final ActorRef wordEndpoint;
    private ActorRef wordCountCoordinator;
    private final ActorRef corpusReceiver;
    private ActorRef trainingCoordinator;
    private final int maxNumberOfLocalWorkers;
    private String localCorpusPartitionPath;

    private WorkerCoordinator(String tempWorkingDir, int maxNumberOfLocalWorkers) {
        this.wordEndpoint = this.context().actorOf(WordEndpoint.props(), WordEndpoint.DEFAULT_NAME);
        this.context().watch(this.wordEndpoint);
        this.corpusReceiver = this.context().actorOf(CorpusReceiver.props(tempWorkingDir));
        this.maxNumberOfLocalWorkers = maxNumberOfLocalWorkers;
    }


    @Override public Receive createReceive() {
        return defaultReceiveBuilder()
                .match(CorpusReceiver.ProcessCorpusPartition.class, this::handle)
                .match(ProcessCorpusChunk.class, this::handle)
                .match(CorpusTransferCompleted.class, this::handle)
                .match(VocabularyReadyForTraining.class, this::handle)
                .match(WordEndpoint.VocabularyCreated.class, this::handle)
                .match(SkipGramReceiver.ProcessEncodedSkipGram.class, this::handle)
                .match(SkipGramReceiver.ProcessUnencodedSkipGrams.class, this::handle)
                .match(TrainingCoordinator.SkipGramChunkTransferred.class, this::handle)
                .match(TrainingCoordinator.EndOfTraining.class, this::handle)
                .match(Terminated.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(CorpusReceiver.ProcessCorpusPartition message) {
        this.corpusReceiver.tell(message, this.sender());
    }

    private void handle(ProcessCorpusChunk message) {
        this.initializeWordCountCoordinator();
        this.wordCountCoordinator.tell(message, this.self());
    }

    private void handle(CorpusTransferCompleted message) {
        this.localCorpusPartitionPath = message.localCorpusPartitionPath;
        this.initializeWordCountCoordinator();
        this.wordCountCoordinator.tell(message, this.self());
        this.sender().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    private void initializeWordCountCoordinator() {
        if (this.wordCountCoordinator == null) {
            this.wordCountCoordinator = this.context().actorOf(
                    WordCountCoordinator.props(this.wordEndpoint, this.maxNumberOfLocalWorkers));
        }
    }

    private void handle(VocabularyReadyForTraining message) {
        this.trainingCoordinator.tell(
                TrainingCoordinator.StartTraining.builder()
                                                 .numberOfLocalWorkers(this.maxNumberOfLocalWorkers)
                                                 .localCorpusPartitionPath(this.localCorpusPartitionPath)
                                                 .build(),
                this.self());
    }

    private void handle(WordEndpoint.VocabularyCreated message) {
        this.trainingCoordinator = this.context().actorOf(TrainingCoordinator.props(this.maxNumberOfLocalWorkers));
    }

    private void handle(Terminated terminated) {
        if (terminated.actor() == this.wordEndpoint) {
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
    }

    // Forwarding

    private void handle(SkipGramReceiver.ProcessEncodedSkipGram message) {
        this.trainingCoordinator.tell(message, this.sender());
    }

    private void handle(SkipGramReceiver.ProcessUnencodedSkipGrams message) {
        this.trainingCoordinator.tell(message, this.sender());
    }

    private void handle(TrainingCoordinator.SkipGramChunkTransferred message) {
        this.trainingCoordinator.tell(message, this.sender());
    }

    private void handle(TrainingCoordinator.EndOfTraining message) {
        this.trainingCoordinator.tell(message, this.sender());
    }

}
