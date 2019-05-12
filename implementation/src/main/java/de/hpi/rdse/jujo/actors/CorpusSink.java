package de.hpi.rdse.jujo.actors;

import akka.actor.Props;
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.SinkRef;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.StreamRefs;
import akka.util.ByteString;
import lombok.AllArgsConstructor;
import lombok.Getter;
import scala.compat.java8.FutureConverters;

import java.io.File;
import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public class CorpusSink extends AbstractReapedActor {

    public static Props props() {
        return Props.create(CorpusSink.class, CorpusSink::new);
    }

    @AllArgsConstructor @Getter
    static class RequestCorpusFromMaster implements Serializable {
        private static final long serialVersionUID = -4024260649244404785L;
        private final File targetDestination;
    }

    private final Materializer materializer;

    private CorpusSink() {
        this.materializer = ActorMaterializer.create(context().system());

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RequestCorpusFromMaster.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(RequestCorpusFromMaster message) {
        Sink<ByteString, CompletionStage<IOResult>> sink = this.createFileSink(message.getTargetDestination());
        CompletionStage<SinkRef<ByteString>> sinkRef = StreamRefs.<ByteString>sinkRef()
                .via(Flow.of(ByteString.class).map(this::processCorpusChunk))
                .to(sink)
                .run(this.materializer);

        Patterns.pipe(FutureConverters.toScala(sinkRef.thenApply(Master.RequestCorpusPartition::new)),
                context().dispatcher())
                .to(this.sender());
    }

    private Sink<ByteString, CompletionStage<IOResult>> createFileSink(File targetDestination) {
        return FileIO.toFile(targetDestination);
    }

    private ByteString processCorpusChunk(ByteString chunk) {
        // TODO: Find a good solution to delegate chunks to a wordCount worker
        return chunk;
    }
}