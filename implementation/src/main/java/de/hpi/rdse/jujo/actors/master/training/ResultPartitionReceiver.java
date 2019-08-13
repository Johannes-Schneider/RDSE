package de.hpi.rdse.jujo.actors.master.training;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.RootActorPath;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.SourceRef;
import com.esotericsoftware.minlog.Log;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.common.training.ResultPartitionSender;
import de.hpi.rdse.jujo.actors.master.Master;
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
import java.util.concurrent.atomic.AtomicInteger;

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
    private final AtomicInteger wordCounts = new AtomicInteger(0);

    private ResultPartitionReceiver(Path resultFile) throws IOException {
        this.writer = new BufferedWriter(new FileWriter(resultFile.toFile(), false));
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
               .watchTermination((notUsed, stage) -> this.handleTermination(notUsed, stage, this.sender()))
               .runForeach(this::handleResult, this.materializer);
    }

    private void handleResult(CWordEmbedding embedding) throws IOException {
        this.writer.write(embedding.toString());
        this.wordCounts.incrementAndGet();
    }

    private NotUsed handleTermination(NotUsed notUsed, CompletionStage<Done> stage, ActorRef sender) {
        stage.whenComplete((done, throwable) -> {
            this.log().info(String.format("Received all results from %s", sender.path().root()));
            this.activeNodes.remove(sender.path().root());
            sender.tell(new ResultPartitionSender.AcknowledgeResultPartition(), this.self());
            if (this.activeNodes.isEmpty()) {
                try {
                    this.writer.flush();
                } catch (IOException e) {
                    Log.error("Flushing of buffered result writer failed.");
                }
                this.context().parent().tell(new Master.AllResultsReceived(), this.self());
                this.purposeHasBeenFulfilled();
            }
        });
        return notUsed;
    }
}
