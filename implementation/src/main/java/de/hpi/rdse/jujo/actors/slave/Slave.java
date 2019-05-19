package de.hpi.rdse.jujo.actors.slave;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.remote.DisassociatedEvent;
import de.hpi.rdse.jujo.actors.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.WordEndpoint;
import de.hpi.rdse.jujo.utils.startup.SlaveCommand;

import java.io.IOException;

public class Slave extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "slave";

    public static Props props(SlaveCommand slaveCommand) {
        return Props.create(Slave.class, () -> new Slave(slaveCommand));
    }

    private final ActorRef wordEndpoint;
    private final ActorRef corpusReceiver;

    @Override
    public void preStart() throws Exception {
        super.preStart();
        this.getContext().getSystem().eventStream().subscribe(this.getSelf(), DisassociatedEvent.class);
    }

    private Slave(SlaveCommand slaveCommand) {
        this.wordEndpoint = this.context().actorOf(WordEndpoint.props(), WordEndpoint.DEFAULT_NAME);
        this.corpusReceiver = this.context().actorOf(CorpusReceiver.props(slaveCommand.getTemporaryWorkingDirectory()));
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                .match(DisassociatedEvent.class, this::handle)
                .match(CorpusReceiver.ProcessCorpusPartition.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(CorpusReceiver.ProcessCorpusPartition message) {
        this.corpusReceiver.tell(message, this.sender());
    }

    private void handle(DisassociatedEvent event) {
        this.log().error("Disassociated startPassword master. Stopping...");
        this.getContext().stop(self());
    }
}
