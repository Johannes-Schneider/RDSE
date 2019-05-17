package de.hpi.rdse.jujo.actors.master;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rdse.jujo.actors.AbstractReapedActor;

import java.util.HashMap;
import java.util.Map;

public class CorpusDistributor extends AbstractReapedActor {

    public static Props props(int expectedNumberOfSlaves, String corpusFilePath) {
        return Props.create(CorpusDistributor.class,
                () -> new CorpusDistributor(expectedNumberOfSlaves, corpusFilePath));
    }

    private final int expectedNumberOfSlaves;
    private final String corpusFilePath;
    private final Map<ActorRef, Object> corpusSources = new HashMap<>();

    private CorpusDistributor(int expectedNumberOfSlaves, String corpusFilePath) {
        this.expectedNumberOfSlaves = expectedNumberOfSlaves;
        this.corpusFilePath = corpusFilePath;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Master.SlaveNodeRegistered.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(Master.SlaveNodeRegistered message) {
        if (!this.corpusSources.containsKey(message.getSlave())) {
            // TODO: Insert corpusSource
        }
        // TODO: Send message including corpusSource
    }
}
