package de.hpi.rdse.jujo.actors;

import akka.actor.AbstractLoggingActor;
import akka.japi.pf.ReceiveBuilder;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public abstract class AbstractReapedActor extends AbstractLoggingActor {

    @NoArgsConstructor
    public static class Resolve implements Serializable {
        private static final long serialVersionUID = -343961504135299385L;
    }

    @NoArgsConstructor
    public static class Resolved implements Serializable {
        private static final long serialVersionUID = -487222029972297207L;
    }

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
                .match(Resolved.class, this::handle);
    }

    private void handle(Resolve message) {
        this.sender().tell(new Resolved(), this.self());
    }

    protected void handle(Resolved message) {
        this.log().info(String.format("Successfully resolved %s", this.sender()));
    }

    protected void handleAny(Object message) {
        this.log().warning(
                String.format("%s received unknown message: %s", this.getClass().getName(), message.toString()));
    }
}