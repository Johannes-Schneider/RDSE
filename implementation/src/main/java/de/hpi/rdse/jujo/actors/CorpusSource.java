package de.hpi.rdse.jujo.actors;

import akka.NotUsed;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import akka.util.ByteString;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.compat.java8.FutureConverters;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;

public class CorpusSource extends AbstractReapedActor {

    private static final long READ_CHUNK_SIZE = 8192;

    public Props props(File inputFile, long readOffset, long readLength) {
        return Props.create(CorpusSource.class, () -> new CorpusSource(inputFile, readOffset, readLength));
    }

    @Builder
    @Getter
    @AllArgsConstructor
    public static class ReceiveCorpusPartition implements Serializable {
        private static final long serialVersionUID = -769066081458075341L;
    }

    // TODO: discuss with Thorsten
    @Getter
    @AllArgsConstructor
    static class CorpusOffer implements Serializable {
        private static final long serialVersionUID = 8680514059027295822L;
        SourceRef<ByteString> sourceRef;
    }

    private final Materializer materializer;
    private final File inputFile;
    private final long readOffset;
    private final long readLength;

    private CorpusSource(File inputFile, long readOffset, long readLength) {
        materializer = ActorMaterializer.create(context().system());
        this.inputFile = inputFile;
        this.readOffset = readOffset;
        this.readLength = readLength;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ReceiveCorpusPartition.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(ReceiveCorpusPartition message) {
        Source<ByteString, NotUsed> source = createSource();
        CompletionStage<SourceRef<ByteString>> sourceRef = source.runWith(StreamRefs.sourceRef(), materializer);

        // TODO: mention bad documentation about Scala vs. Java futures
        Patterns.pipe(FutureConverters.toScala(sourceRef.thenApply(CorpusOffer::new)), context().dispatcher())
                .to(sender());
    }

    private Source<ByteString, NotUsed> createSource() {
        try {
            FileInputStream stream = new FileInputStream(inputFile);
            return Source.fromIterator(
                    () -> new FileIterator(stream, readOffset, readLength, READ_CHUNK_SIZE));

        } catch (FileNotFoundException e) {
            this.log().error(e, "unable to create corpus source");
            return Source.empty();
        }
    }

    @AllArgsConstructor
    static class FileIterator implements Iterator<ByteString> {

        private static final Logger Log = LogManager.getLogger(FileIterator.class);

        private final FileInputStream fileStream;
        private final long chunkSize;
        private long readOffset;
        private long readLength;

        @Override
        public boolean hasNext() {
            return readLength > 0;
        }

        @Override
        public ByteString next() {
            try {
                fileStream.getChannel().position(readOffset);
                int bufferSize = (int) Math.min(chunkSize, readLength);
                byte[] buffer = new byte[bufferSize];
                long read = (long) fileStream.read(buffer);
                readOffset += read;
                readLength -= read;

                return ByteString.fromByteBuffer(ByteBuffer.wrap(buffer));
            } catch (IOException e) {
                Log.error("exception while reading from corpus file", e);
                readLength = 0;
                return ByteString.empty();
            }
        }
    }
}
