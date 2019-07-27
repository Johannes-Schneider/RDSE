package de.hpi.rdse.jujo.actors.slave;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Address;
import akka.actor.Cancellable;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Scheduler;
import akka.actor.Terminated;
import akka.remote.DisassociatedEvent;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.master.Shepherd;
import de.hpi.rdse.jujo.training.Word2VecConfiguration;
import de.hpi.rdse.jujo.training.Word2VecModel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class Sheep extends AbstractReapedActor {

    public static Props props(ActorRef slave) {
        return Props.create(Sheep.class, () -> new Sheep(slave));
    }

    @Getter @Builder @AllArgsConstructor @NoArgsConstructor
    public static class RegisterAtShepherd implements Serializable {
        private static final long serialVersionUID = -4399047760637406556L;
        private Address shepherdAddress;
    }

    @NoArgsConstructor @AllArgsConstructor @Getter
    public static class AcknowledgeRegistration implements Serializable {
        private static final long serialVersionUID = 3226726675135579564L;
        private Word2VecConfiguration word2VecConfiguration;
    }

    private Cancellable connectSchedule;
    private final ActorRef slave;

    private Sheep(ActorRef slave) {
        this.slave = slave;
        this.context().watch(this.slave);
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(RegisterAtShepherd.class, this::handle)
                   .match(AcknowledgeRegistration.class, this::handle)
                   .match(DisassociatedEvent.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private void handle(RegisterAtShepherd message) {
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
                () -> selection.tell(new Shepherd.SlaveNodeRegistrationMessage(this.slave), this.self()),
                dispatcher
        );
    }

    private void handle(AcknowledgeRegistration message) {
        this.cancelRunningConnectSchedule();
        this.log().info("Subscription successfully acknowledged by {}.", this.getSender());
        Word2VecModel.setModelConfiguration(message.getWord2VecConfiguration());
    }

    private void cancelRunningConnectSchedule() {
        if (this.connectSchedule != null) {
            this.connectSchedule.cancel();
            this.connectSchedule = null;
        }
    }

    @Override
    protected void handleTerminated(Terminated message) {
        super.handleTerminated(message);

        if (message.actor() == this.slave) {
            this.purposeHasBeenFulfilled();
        }
    }

    private void handle(DisassociatedEvent event) {
        if (this.connectSchedule == null) {
            this.log().error("Disassociated from master. Stopping Sheep ...");
            this.getContext().stop(self());
        }
    }
}
