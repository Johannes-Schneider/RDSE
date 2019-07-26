package de.hpi.rdse.jujo.actors.master;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.common.WordEndpoint;

import java.util.LinkedList;
import java.util.List;

public class WordEndpointDistributor extends AbstractReapedActor {

    public static Props props(int expectedNumberOfWordEndpoints) {
        return Props.create(WordEndpointDistributor.class,
                () -> new WordEndpointDistributor(expectedNumberOfWordEndpoints));
    }

    private final int expectedNumberOfWordEndpoints;
    private final List<ActorRef> wordEndpoints = new LinkedList<>();

    private WordEndpointDistributor(int expectedNumberOfWordEndpoints) {
        this.expectedNumberOfWordEndpoints = expectedNumberOfWordEndpoints;
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(Shepherd.SlaveNodeRegistrationMessage.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private void handle(Shepherd.SlaveNodeRegistrationMessage message) {
        ActorSelection wordEndpoint = this.context().system().actorSelection(
                message.getSlave().path() + "/*/" + WordEndpoint.DEFAULT_NAME);
        wordEndpoint.tell(new AbstractReapedActor.Resolve(), this.self());
    }

    @Override
    protected final void handle(AbstractReapedActor.Resolved message) {
        super.handle(message);
        if (this.wordEndpoints.contains(this.sender())) {
            return;
        }
        this.wordEndpoints.add(this.sender());
        if (this.expectedNumberOfWordEndpoints == this.wordEndpoints.size()) {
            this.distributeWordEndpoints();
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
    }

    private void distributeWordEndpoints() {
        WordEndpoint.WordEndpoints message = WordEndpoint.WordEndpoints
                .builder()
                .endpoints(this.wordEndpoints)
                .master(this.context().parent())
                .build();
        for (ActorRef wordEndpoint : this.wordEndpoints) {
            wordEndpoint.tell(message, this.self());
        }
    }

}
