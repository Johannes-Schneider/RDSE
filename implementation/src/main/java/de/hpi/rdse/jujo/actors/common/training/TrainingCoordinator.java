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

    public static Props props() {
        return Props.create(TrainingCoordinator.class, TrainingCoordinator::new);
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

    private TrainingCoordinator() {

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
        // TODO: start training
    }

    private void handle(SkipGramsDistributed message) {
        this.sender().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    private void handle(SkipGramReceiver.ProcessSkipGrams message) {
        this.skipGramReceiver.tell(message, this.sender());
    }
}
