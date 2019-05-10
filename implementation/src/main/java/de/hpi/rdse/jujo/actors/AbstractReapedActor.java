package de.hpi.rdse.jujo.actors;

import akka.actor.AbstractLoggingActor;

public abstract class AbstractReapedActor extends AbstractLoggingActor {

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

    protected void handleAny(Object message) {
        this.log().warning(String.format("%s received unknown message: %s", this.getClass().getName(), message.toString()));
    }
}