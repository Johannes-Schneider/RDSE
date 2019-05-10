package de.hpi.rdse.jujo.actors;

import akka.actor.ActorRef;
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
import scala.compat.java8.FutureConverters;

import java.io.File;
import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public class CorpusSink extends AbstractReapedActor {

    public static Props props(File storageFile, ActorRef wordCountWorker) {
        return Props.create(CorpusSink.class, () -> new CorpusSink(storageFile, wordCountWorker));
    }

    static class PrepareCorpusTransferMessage implements Serializable {
        private static final long serialVersionUID = -4024260649244404785L;
    }

    private final Materializer materializer;
    private final File storageFile;
    private final ActorRef wordCountWorker;

    private CorpusSink(File storageFile, ActorRef wordCountWorker) {
        materializer = ActorMaterializer.create(context().system());
        this.storageFile = storageFile;
        this.wordCountWorker = wordCountWorker;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(PrepareCorpusTransferMessage.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(PrepareCorpusTransferMessage message) {
        Sink<ByteString, CompletionStage<IOResult>> sink = createSink();
        CompletionStage<SinkRef<ByteString>> sinkRef = StreamRefs.<ByteString>sinkRef()
                .via(Flow.of(ByteString.class).map(this::processCorpusChunk))
                .to(sink)
                .run(materializer);

        Patterns.pipe(FutureConverters.toScala(sinkRef.thenApply(CorpusSource.CorpusSinkReady::new)), context().dispatcher())
                .to(sender());
    }

    private Sink<ByteString, CompletionStage<IOResult>> createSink() {
        return FileIO.toFile(storageFile);
    }

    private ByteString processCorpusChunk(ByteString chunk) {
        wordCountWorker.tell(WordCountWorker.ProcessCorpusChunk.builder().chunk(chunk).build(), self());
        return chunk;
    }
}
