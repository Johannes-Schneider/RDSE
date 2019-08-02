package de.hpi.rdse.jujo.streaming;

import akka.actor.ActorRef;
import akka.stream.SinkRef;
import akka.util.ByteString;

public interface StreamInputBufferFactory {

    StreamInputBuffer createStreamInputBufferFor(ActorRef remote, SinkRef<ByteString> akkaSink);
}
