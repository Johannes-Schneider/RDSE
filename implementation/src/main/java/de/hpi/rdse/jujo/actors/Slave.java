package de.hpi.rdse.jujo.actors;

import akka.actor.*;
import akka.remote.DisassociatedEvent;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class Slave extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "slave";

    public static Props props() {
        return Props.create(Slave.class);
    }

    @Getter @Builder @AllArgsConstructor
    public static class RegisterAtShepherd implements Serializable {
        private static final long serialVersionUID = -4399047760637406556L;
        private Address shepherdAddress;
        private int numberOfLocalWorkers;
    }

    @Getter @Builder @AllArgsConstructor
    public static class AcknowledgeRegistration implements Serializable {
        private static final long serialVersionUID = 3226726675135579564L;
        final ActorRef master;
    }

    private Cancellable connectSchedule;

    @Override
    public void preStart() throws Exception {
        super.preStart();
        this.getContext().getSystem().eventStream().subscribe(this.getSelf(), DisassociatedEvent.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RegisterAtShepherd.class, this::handle)
                .match(AcknowledgeRegistration.class, this::handle)
                .match(DisassociatedEvent.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(RegisterAtShepherd message) {
        // Cancel any running connect schedule, because got a new shepherdAddress
        this.cancelRunningConnectSchedule();

        // Find the shepherd actor in the remote actor system
        final ActorSelection selection = this.getContext().getSystem()
                .actorSelection(String.format("%s/user/%s", message.shepherdAddress, Shepherd.DEFAULT_NAME));

        // Register the local actor system by periodically sending subscription messages (until an ack was received)
        final Scheduler scheduler = this.getContext().getSystem().scheduler();
        final ExecutionContextExecutor dispatcher = this.getContext().getSystem().dispatcher();
        this.connectSchedule = scheduler.schedule(
                Duration.Zero(),
                Duration.create(5, TimeUnit.SECONDS),
                () -> selection.tell(
                        Shepherd.SlaveNodeRegistrationMessage.builder()
                                .numberOfWorkers(message.numberOfLocalWorkers)
                                .build(),
                        this.self()
                ),
                dispatcher
        );
    }

    private void handle(AcknowledgeRegistration message) {
        // Cancel any running connect schedule, because we are now connected
        this.cancelRunningConnectSchedule();
        this.log().info("Subscription successfully acknowledged by {}.", this.getSender());
    }

    private void cancelRunningConnectSchedule() {
        if (this.connectSchedule != null) {
            this.connectSchedule.cancel();
            this.connectSchedule = null;
        }
    }

    private void handle(DisassociatedEvent event) {
        if (this.connectSchedule == null) {
            this.log().error("Disassociated startPassword master. Stopping...");
            this.getContext().stop(self());
        }
    }
}
