package de.hpi.rdse.jujo.actors.common;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.japi.pf.ReceiveBuilder;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public abstract class AbstractReapedActor extends AbstractLoggingActor {

    @NoArgsConstructor
    public static class Resolve implements Serializable {
        private static final long serialVersionUID = -343961504135299385L;
    }

    @NoArgsConstructor
    public static class Resolved implements Serializable {
        private static final long serialVersionUID = -487222029972297207L;
    }

    private final Set<ActorRef> childActors = new HashSet<>();
    private boolean isPurposeFulfilled = false;

    @Override
    public void preStart() throws Exception {
        super.preStart();
        Reaper.watchWithDefaultReaper(this);
        this.log().debug(String.format("%s has been started", this.self()));
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        this.log().debug(String.format("%s has been stopped.", this.self()));
    }

    protected ReceiveBuilder defaultReceiveBuilder() {
        return this.receiveBuilder()
                   .match(Resolve.class, this::handle)
                   .match(Resolved.class, this::handle)
                   .match(Terminated.class, this::handleTerminated);
    }

    protected final void logProcessStep(String message) {
        this.log().info(String.format("##################### %s #####################", message));
    }

    protected void ignore(Object message) {
        // message is ignored
    }

    private void handle(Resolve message) {
        this.sender().tell(new Resolved(), this.self());
    }

    protected void handle(Resolved message) {
        this.log().info(String.format("Successfully resolved %s", this.sender()));
    }

    protected void handleTerminated(Terminated message) {
        if (!this.childActors.remove(message.actor())) {
            this.log().warning(String.format("Received Terminated message from non-child actor (%s)!",
                                             message.actor().path()));
        }

        this.context().unwatch(message.actor());
        if (this.childActors.isEmpty() && this.isPurposeFulfilled) {
            this.terminateSelf();
        }
    }

    protected final boolean isPurposeFulfilled() {
        return this.isPurposeFulfilled;
    }

    protected final void purposeHasBeenFulfilled() {
        this.log().debug(String.format("Purpose of %s has been fulfilled", this.self().path()));
        this.isPurposeFulfilled = true;

        if (this.childActors.isEmpty()) {
            this.terminateSelf();
            return;
        }

        this.log().debug(String.format("Purpose has been fulfilled, but we are still waiting for %d children to also " +
                                       "terminate", this.childActors.size()));
    }

    protected void handleAny(Object message) {
        this.log().warning(
                String.format("%s received unknown message: %s", this.getClass().getName(), message.toString()));
    }

    protected ActorRef spawnChild(Props props) {
        return this.spawnChild(props, String.format("%s-%s", props.actorClass().getSimpleName(), UUID.randomUUID()));
    }

    protected ActorRef spawnChild(Props props, String name) {
        ActorRef childActor = this.context().actorOf(props, name);
        this.context().watch(childActor);
        this.childActors.add(childActor);
        return childActor;
    }

    private void terminateSelf() {
        this.log().debug(String.format("Terminating self (%s).", this.self().path()));
        this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }
}