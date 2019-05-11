package de.hpi.rdse.jujo.actors;

import akka.actor.*;
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
        if (!this.slaves.add(sender())) {
            return;
        }
        this.log().info(String.format("New subscription: %s with %d available workers", sender(), message.numberOfWorkers));

        this.sender().tell(Slave.AcknowledgeRegistration.builder().master(master).build(), self());
        this.context().watch(sender());

        master.tell(
                Master.SlaveNodeRegistered.builder()
                        .slaveAddress(this.sender().path().address())
                        .numberOfWorkers(message.numberOfWorkers)
                        .build(),
                self()
        );
    }

    private void handle(Terminated message) {
        if (sender() == master) {
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
            return;
        }

        slaves.remove(this.sender());
        master.tell(Master.SlaveNodeTerminated.builder()
                        .slaveAddress(this.sender().path().address())
                        .build(), this.self());
    }
}