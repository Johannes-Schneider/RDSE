package de.hpi.rdse.jujo.actors.common;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.japi.pf.ReceiveBuilder;
import akka.routing.Router;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractReapedActor extends AbstractLoggingActor {

    @NoArgsConstructor
    public static class Resolve implements Serializable {
        private static final long serialVersionUID = -343961504135299385L;
    }

    @NoArgsConstructor
    public static class Resolved implements Serializable {
        private static final long serialVersionUID = -487222029972297207L;
    }

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

    private void handle(Resolve message) {
        this.sender().tell(new Resolved(), this.self());
    }

    protected void handle(Resolved message) {
        this.log().info(String.format("Successfully resolved %s", this.sender()));
    }

    protected void handleTerminated(Terminated message) {
        if (!this.context().children().toStream().contains(message.actor())) {
            this.log().warning("Received Terminated message from non-child actor!");
        }

        this.context().unwatch(message.actor());

        if (this.context().children().isEmpty() && this.isPurposeFulfilled) {
            this.terminateSelf();
        }
    }

    protected final boolean isPurposeFulfilled() {
        return this.isPurposeFulfilled;
    }

    protected final void purposeHasBeenFulfilled() {
        this.log().debug(String.format("Purpose of %s has been fulfilled", this.self().path()));
        this.isPurposeFulfilled = true;

        if (this.context().children().isEmpty()) {
            this.terminateSelf();
        }
    }

    protected void handleAny(Object message) {
        this.log().warning(
                String.format("%s received unknown message: %s", this.getClass().getName(), message.toString()));
    }

    protected ActorRef spawnChild(Props props) {
        ActorRef childActor = this.context().actorOf(props);
        this.context().watch(childActor);
        return childActor;
    }

    protected ActorRef spawnChild(Props props, String name) {
        ActorRef childActor = this.context().actorOf(props, name);
        this.context().watch(childActor);
        return childActor;
    }

    protected final boolean removeFromRouter(AtomicReference<Router> router, ActorRef actor) {
        int numberOfRoutees = router.get().routees().size();
        router.set(router.get().removeRoutee(actor));

        return router.get().routees().size() < numberOfRoutees;
    }

    private void terminateSelf() {
        this.log().debug(String.format("Terminating self (%s).", this.self().path()));
        this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }
}