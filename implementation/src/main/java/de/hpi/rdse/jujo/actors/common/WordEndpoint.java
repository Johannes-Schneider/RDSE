package de.hpi.rdse.jujo.actors.common;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WordEndpoint extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "WordEndpoint";

    public static Props props() {
        return Props.create(WordEndpoint.class, WordEndpoint::new);
    }

    @Builder @NoArgsConstructor @AllArgsConstructor @Getter
    public static class WordEndpoints implements Serializable {
        private static final long serialVersionUID = 8070089151352318828L;
        private List<ActorRef> endpoints;
    }

    @AllArgsConstructor @NoArgsConstructor @Getter
    public static class WordsCounted implements Serializable {
        private static final long serialVersionUID = -5661255174425103187L;
        private Map<String, Long> wordCounts;
    }

    private final List<ActorRef> wordEndpoints = new ArrayList<>();

    private WordEndpoint() { }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                .match(WordEndpoints.class, this::handle)
                .match(WordsCounted.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(WordEndpoints message) {
        if (!this.wordEndpoints.isEmpty()) {
            this.log().warning("Received WordEndpoints message although already received earlier.");
            this.wordEndpoints.clear();
        }

        this.wordEndpoints.addAll(message.getEndpoints());
    }

    private void handle(WordsCounted message) {
        this.sender().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }
}
