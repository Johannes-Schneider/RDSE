package de.hpi.rdse.jujo.streaming;

import akka.actor.ActorRef;
import akka.stream.SinkRef;
import akka.util.ByteString;

public class DefaultStreamInputBufferFactory implements StreamInputBufferFactory {

    @Override
    public StreamInputBuffer createStreamInputBufferFor(ActorRef remote, SinkRef<ByteString> akkaSink) {
        return null;
    }
}
