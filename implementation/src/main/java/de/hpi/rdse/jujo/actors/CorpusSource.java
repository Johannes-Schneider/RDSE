package de.hpi.rdse.jujo.actors;

import akka.NotUsed;
import akka.actor.Props;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.SinkRef;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class CorpusSource extends AbstractReapedActor {

    private static final long READ_CHUNK_SIZE = 8192;

    @Getter
    @Builder
    static class TransferPartition implements Serializable {
        private static final long serialVersionUID = 4382490549365244631L;
        final SinkRef<ByteString> sinkRef;
    }

    public static Props props(File inputFile, long readOffset, long readLength) {
        return Props.create(CorpusSource.class, () -> new CorpusSource(inputFile, readOffset, readLength));
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
                .matchAny(this::handleAny)
                .match(TransferPartition.class, this::handle)
                .build();
    }

    private void handle(TransferPartition message) {
        this.createSource().runWith(message.getSinkRef().getSink(), materializer);
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
