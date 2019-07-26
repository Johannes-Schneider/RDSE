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

import java.util.concurrent.CompletionStage;

public class ResultPartitionSender extends AbstractReapedActor {

    public static Props props() {
        return Props.create(ResultPartitionSender.class, ResultPartitionSender::new);
    }

    private final Materializer materializer;

    private ResultPartitionSender() {
        this.materializer = ActorMaterializer.create(this.context().system());
        this.transferResults();
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .matchAny(this::handleAny)
                   .build();
    }

    private void transferResults() {
        Source<CWordEmbedding, NotUsed> source = createResultPartitionSource();
        CompletionStage<SourceRef<CWordEmbedding>> sourceRef = source.runWith(StreamRefs.sourceRef(),
                this.materializer);
        Patterns.pipe(sourceRef.thenApply(ResultPartitionReceiver.ProcessResults::new),
                this.context().dispatcher()).to(WordEndpointResolver.getInstance().getMaster(), this.self());
    }


    private Source<CWordEmbedding, NotUsed> createResultPartitionSource() {
        return Source.<CWordEmbedding>fromIterator(Word2VecModel.getInstance()::getResults);
    }

}
