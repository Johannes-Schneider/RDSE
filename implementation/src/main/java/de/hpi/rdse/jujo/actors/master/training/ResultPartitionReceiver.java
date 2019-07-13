package de.hpi.rdse.jujo.actors.master.training;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.RootActorPath;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.SourceRef;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.training.CWordEmbedding;
import de.hpi.rdse.jujo.wordManagement.WordEndpointResolver;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;

public class ResultPartitionReceiver extends AbstractReapedActor {

    public static Props props(Path resultFile) {
        return Props.create(ResultPartitionReceiver.class, () -> new ResultPartitionReceiver(resultFile));
    }

    @Getter @AllArgsConstructor @NoArgsConstructor
    public static class ProcessResults implements Serializable {
        private static final long serialVersionUID = -8923814029758785155L;
        private SourceRef<CWordEmbedding> source;
    }

    private final BufferedWriter writer;
    private final Set<RootActorPath> activeNodes = new HashSet<>();
    private final Materializer materializer;

    private ResultPartitionReceiver(Path resultFile) throws IOException {
        this.writer = new BufferedWriter(new FileWriter(resultFile.toFile(), true));
        this.materializer = ActorMaterializer.create(this.context().system());
        for (ActorRef endpoint : WordEndpointResolver.getInstance().all()) {
            this.activeNodes.add(endpoint.path().root());
        }
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(ProcessResults.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private void handle(ProcessResults message) {

        message.getSource().getSource()
               .watchTermination((notUsed, stage) -> this.handleTermination(notUsed, stage, this.sender().path().root()))
               .runForeach(this::handleResult, this.materializer);
    }

    private void handleResult(CWordEmbedding embedding) throws IOException {
        writer.write(embedding.toString());
        writer.newLine();
    }

    private NotUsed handleTermination(NotUsed notUsed, CompletionStage<Done> stage, RootActorPath sender) {
        stage.thenApply(x -> {

            return x;
        });
        return notUsed;
    }
}
