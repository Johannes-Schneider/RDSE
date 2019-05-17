package de.hpi.rdse.jujo.actors.slave;

import akka.actor.Props;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.Sink;
import akka.util.ByteString;
import de.hpi.rdse.jujo.actors.AbstractReapedActor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Paths;

public class CorpusReceiver extends AbstractReapedActor {

    private static final String CORPUS_DIRECTORY_NAME = "corpus";
    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    public static Props props(String temporaryWorkingDirectory) {
        return Props.create(CorpusReceiver.class, () -> new CorpusReceiver(temporaryWorkingDirectory));
    }

    @Builder @NoArgsConstructor @AllArgsConstructor @Getter
    public static class ProcessCorpusPartition implements Serializable {
        private static final long serialVersionUID = -7417712897300453585L;
        private SourceRef<ByteString> source;
    }

    private final File corpusLocation;
    private final Materializer materializer;

    private CorpusReceiver(String temporaryWorkingDirectory) throws IOException {
        this.corpusLocation = this.createLocalWorkingDirectory(temporaryWorkingDirectory);
        this.materializer = ActorMaterializer.create(this.context().system());
    }

    private File createLocalWorkingDirectory(String temporaryWorkingDirectory) throws IOException {
        File corpusLocation = Paths.get(temporaryWorkingDirectory, CORPUS_DIRECTORY_NAME).toFile();
        if (corpusLocation.exists()) {
            return corpusLocation;
        }
        if (!this.corpusLocation.mkdir()) {
            throw new IOException("Unable to create directory for storing corpus. Check file system permissions.");
        }
        return corpusLocation;
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                .match(ProcessCorpusPartition.class, this::handle)
                .build();
    }

    private void handle(ProcessCorpusPartition message) {
        message.getSource().getSource().runWith(Sink.foreach(this::processCorpusChunk), this.materializer);
    }

    private void processCorpusChunk(ByteString chunk) {
        System.out.println(chunk.decodeString(DEFAULT_CHARSET));
    }
}
