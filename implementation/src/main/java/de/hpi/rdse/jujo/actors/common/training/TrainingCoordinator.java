package de.hpi.rdse.jujo.actors.common.training;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public class TrainingCoordinator extends AbstractReapedActor {

    public static Props props(int windowSize) {
        return Props.create(TrainingCoordinator.class, () -> new TrainingCoordinator(windowSize));
    }

    @NoArgsConstructor @AllArgsConstructor @Builder @Getter
    public static class StartTraining implements Serializable {
        private static final long serialVersionUID = -910991812790625629L;
        private Vocabulary vocabulary;
        private int numberOfLocalWorkers;
        private String localCorpusPartitionPath;
    }

    @NoArgsConstructor
    public static class SkipGramsDistributed implements Serializable {
        private static final long serialVersionUID = 4150057273673434932L;
    }

    private ActorRef skipGramDistributor;
    private ActorRef skipGramReceiver;
    private int windowSize;

    private TrainingCoordinator(int windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(StartTraining.class, this::handle)
                   .match(SkipGramsDistributed.class, this::handle)
                   .match(SkipGramReceiver.ProcessSkipGrams.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private void handle(StartTraining message) {
        this.initializeSkipGramDistributor(message.getLocalCorpusPartitionPath(), message.getVocabulary());
        // TODO: May end up in race conditions: First skip-grams may be received without receiver being present.
        this.initializeSkipGramReceiver(message.getVocabulary());
        // TODO: start training
    }

    private void initializeSkipGramDistributor(String localCorpusPartitionPath, Vocabulary vocabulary) {
        if (this.skipGramDistributor == null) {
            return;
        }
        this.skipGramDistributor = this.context().actorOf(SkipGramDistributor.props(localCorpusPartitionPath,
                vocabulary, this.windowSize));
    }

    private void initializeSkipGramReceiver(Vocabulary vocabulary) {
        if (this.skipGramDistributor == null) {
            return;
        }
        this.skipGramReceiver = this.context().actorOf(SkipGramReceiver.props(vocabulary));
    }

    private void handle(SkipGramsDistributed message) {
        this.sender().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    private void handle(SkipGramReceiver.ProcessSkipGrams message) {
        this.skipGramReceiver.tell(message, this.sender());
    }
}
