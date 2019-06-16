package de.hpi.rdse.jujo.actors.master;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.slave.Sheep;
import de.hpi.rdse.jujo.startup.MasterCommand;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Shepherd extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "shepherd";

    public static Props props(final ActorRef master, final MasterCommand masterCommand) {
        return Props.create(Shepherd.class, () -> new Shepherd(master, masterCommand));
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    public static class SlaveNodeRegistrationMessage implements Serializable {
        private static final long serialVersionUID = 2517545349430030374L;
        private ActorRef slave;
    }

    private final ActorRef master;
    private final Set<ActorRef> slaves = new HashSet<>();
    private final MasterCommand masterCommand;

    private Shepherd(final ActorRef master, final MasterCommand masterCommand) {
        this.master = master;
        this.masterCommand = masterCommand;
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
        return this.defaultReceiveBuilder()
                .match(SlaveNodeRegistrationMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(SlaveNodeRegistrationMessage message) {
        if (!this.slaves.add(this.sender())) {
            return;
        }
        this.log().info(String.format("New subscription: %s with available workers", this.sender()));

        this.sender().tell(new Sheep.AcknowledgeRegistration(this.masterCommand.getWindowSize()), this.self());
        this.context().watch(sender());
        this.master.tell(message, this.self());
    }

    private void handle(Terminated message) {
        if (this.sender() == this.master) {
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
            return;
        }

        this.slaves.remove(this.sender());
        this.master.tell(Master.SlaveNodeTerminated.builder().slave(this.sender()).build(), this.self());
    }
}