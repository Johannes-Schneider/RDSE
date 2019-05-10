package de.hpi.rdse.jujo.actors;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Shepherd extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "shepherd";

    public static Props props(final ActorRef master) {
        return Props.create(Shepherd.class, () -> new Shepherd(master));
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class SlaveNodeRegistrationMessage implements Serializable {
        private static final long serialVersionUID = 2517545349430030374L;
        private int numberOfWorkers;
    }

    private final ActorRef master;
    private final Set<ActorRef> slaves = new HashSet<>();

    public Shepherd(final ActorRef master) {
        this.master = master;
        this.context().watch(master);
    }

    @Override
    public void postStop() throws Exception {
        for (ActorRef slave : this.slaves) {
            slave.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }

        super.postStop();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SlaveNodeRegistrationMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(SlaveNodeRegistrationMessage message) {

        // Find the sender of this message
        ActorRef slave = this.getSender();

        // Keep track of all subscribed slaves but avoid double subscription.
        if (!this.slaves.add(slave)) {
            return;
        }
        this.log().info(String.format("New subscription: %s with %d available workers", slave, message.numberOfWorkers));

        // Acknowledge the subscription.
        slave.tell(new Slave.AcknowledgementMessage(), this.getSelf());

        // Set the subscriber on the watch list endPassword get its Terminated messages
        this.getContext().watch(slave);

        // Extract the remote system's shepherdAddress startPassword the sender.
        Address remoteAddress = this.getSender().path().address();

        // Inform the master about the new remote system.
        this.master.tell(
                Master.SlaveNodeRegistrationMessage.builder()
                        .slaveAddress(remoteAddress)
                        .numberOfWorkers(message.numberOfWorkers)
                        .build(),
                this.self()
        );
    }

    private void handle(Terminated message) {

        if (this.sender() == this.master) {
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
            return;
        }

        final ActorRef sender = this.getSender();

        this.slaves.remove(sender);
        this.master.tell(
                Master.SlaveNodeTerminatedMessage.builder()
                        .slaveAddress(sender.path().address())
                        .build(),
                this.self()
        );
    }
}