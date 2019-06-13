package de.hpi.rdse.jujo.actors.slave;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Flow;
import akka.util.ByteString;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.common.WorkerCoordinator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;

public class CorpusReceiver extends AbstractReapedActor {

    private static final String CORPUS_DIRECTORY_NAME = "corpus";
    private static final String CORPUS_FILE_NAME = "corpus.txt";

    public static Props props(ActorRef supervisor, String temporaryWorkingDirectory) {
        return Props.create(CorpusReceiver.class, () -> new CorpusReceiver(supervisor, temporaryWorkingDirectory));
    }

    @Builder @NoArgsConstructor @AllArgsConstructor @Getter
    public static class ProcessCorpusPartition implements Serializable {
        private static final long serialVersionUID = -7417712897300453585L;
        private SourceRef<ByteString> source;
    }

    private final ActorRef supervisor;
    private final File corpusLocation;
    private final Materializer materializer;

    private CorpusReceiver(ActorRef supervisor, String temporaryWorkingDirectory) throws IOException {
        this.supervisor = supervisor;
        this.corpusLocation = this.createLocalWorkingDirectory(temporaryWorkingDirectory);
        this.materializer = ActorMaterializer.create(this.context());
    }

    private File createLocalWorkingDirectory(String temporaryWorkingDirectory) throws IOException {
        File corpusLocation = Paths.get(temporaryWorkingDirectory, CORPUS_DIRECTORY_NAME).toFile();
        if (corpusLocation.exists()) {
            return corpusLocation;
        }
        if (!corpusLocation.mkdirs()) {
            throw new IOException("Unable to create directory for storing corpus. Check file system permissions.");
        }
        return corpusLocation;
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                .match(ProcessCorpusPartition.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(ProcessCorpusPartition message) {
        message.getSource().getSource()
                .via(Flow.of(ByteString.class).map(this::handleCorpusChunk))
                .watchTermination(this::handleTermination)
                .runWith(FileIO.toPath(Paths.get(this.corpusLocation.getPath(), CORPUS_FILE_NAME)), this.materializer);
    }

    private ByteString handleCorpusChunk(ByteString chunk) {
        this.log().info(String.format("Received chunk of size %d", chunk.size()));
        this.supervisor.tell(new WorkerCoordinator.ProcessCorpusChunk(chunk), this.self());
        return chunk;
    }

    private NotUsed handleTermination(NotUsed not, CompletionStage<Done> stage) {
        stage.thenApply(x -> {
            this.supervisor.tell(new WorkerCoordinator.CorpusTransferCompleted(
                    Paths.get(this.corpusLocation.getPath(), CORPUS_FILE_NAME).toString()), this.self());
            return x;
        });
        return not;
    }
}
