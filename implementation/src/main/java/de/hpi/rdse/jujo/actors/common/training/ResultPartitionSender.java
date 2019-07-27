package de.hpi.rdse.jujo.actors.common.training;

import akka.NotUsed;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.master.training.ResultPartitionReceiver;
import de.hpi.rdse.jujo.training.CWordEmbedding;
import de.hpi.rdse.jujo.training.Word2VecModel;
import de.hpi.rdse.jujo.wordManagement.WordEndpointResolver;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public class ResultPartitionSender extends AbstractReapedActor {

    public static Props props() {
        return Props.create(ResultPartitionSender.class, ResultPartitionSender::new);
    }

    @NoArgsConstructor
    public static class StartTransfer implements Serializable {
        private static final long serialVersionUID = 720148360700381725L;
    }

    @NoArgsConstructor
    public static class AcknowledgeResultPartition implements Serializable {
        private static final long serialVersionUID = -7845095715553067751L;
    }

    private final Materializer materializer;

    private ResultPartitionSender() {
        this.materializer = ActorMaterializer.create(this.context().system());
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(StartTransfer.class, this::handle)
                   .match(AcknowledgeResultPartition.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private void handle(StartTransfer message) {
        Source<CWordEmbedding, NotUsed> source = createResultPartitionSource();
        CompletionStage<SourceRef<CWordEmbedding>> sourceRef = source.runWith(StreamRefs.sourceRef(),
                                                                              this.materializer);
        Patterns.pipe(sourceRef.thenApply(ResultPartitionReceiver.ProcessResults::new),
                      this.context().dispatcher()).to(WordEndpointResolver.getInstance().getMaster(), this.self());
    }

    private Source<CWordEmbedding, NotUsed> createResultPartitionSource() {
        return Source.<CWordEmbedding>fromIterator(Word2VecModel.getInstance()::getResults);
    }

    private void handle(AcknowledgeResultPartition message) {
        this.purposeHasBeenFulfilled();
    }
}
