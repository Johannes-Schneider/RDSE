package de.hpi.rdse.jujo.actors.master;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rdse.jujo.actors.AbstractReapedActor;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashMap;
import java.util.Map;

public class WordRangeDistributor extends AbstractReapedActor {

    public static Props props(int expectedNumberOfSlaves) {
        return Props.create(WordRangeDistributor.class, () -> new WordRangeDistributor(expectedNumberOfSlaves));
    }

    private final int expectedNumberOfSlaves;
    private final Map<ActorRef, Object> wordRanges = new HashMap<>();

    private WordRangeDistributor(int expectedNumberOfSlaves) {
        this.expectedNumberOfSlaves = expectedNumberOfSlaves;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Master.SlaveNodeRegistered.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(Master.SlaveNodeRegistered message) {
        if (!this.wordRanges.containsKey(message.getSlave())) {
            // TODO Insert wordRange
        }
        if (this.expectedNumberOfSlaves == this.wordRanges.size()) {
            this.distributeWordRanges();
        }
    }

    private void distributeWordRanges() {
        // TODO
        throw new NotImplementedException();
    }

}
