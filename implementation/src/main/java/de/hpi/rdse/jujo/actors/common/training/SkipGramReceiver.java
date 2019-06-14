package de.hpi.rdse.jujo.actors.common.training;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public class SkipGramReceiver extends AbstractReapedActor {

    public static Props props(ActorRef supervisor, Vocabulary vocabulary) {
        return Props.create(SkipGramReceiver.class, () -> new SkipGramReceiver(supervisor, vocabulary));
    }

    @NoArgsConstructor @Getter
    public static class ProcessSkipGrams implements Serializable {
        private static final long serialVersionUID = 735332284132943544L;
    }

    private final ActorRef supervisor;
    private final Vocabulary vocabulary;

    private SkipGramReceiver(ActorRef supervisor, Vocabulary vocabulary) {
        this.supervisor = supervisor;
        this.vocabulary = vocabulary;
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(ProcessSkipGrams.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private void handle(ProcessSkipGrams message) {

    }
}
