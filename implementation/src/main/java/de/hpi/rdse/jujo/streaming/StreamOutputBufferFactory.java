package de.hpi.rdse.jujo.streaming;

import akka.actor.ActorRef;

public interface StreamOutputBufferFactory {

    StreamOutputBuffer createStreamOutputBufferFor(ActorRef remote);
}
