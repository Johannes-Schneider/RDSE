package de.hpi.rdse.jujo.actors.common;

import akka.Done;
import akka.NotUsed;
import akka.actor.Props;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class VocabularyReceiver extends AbstractReapedActor {

    public static Props props(Vocabulary vocabulary) {
        return Props.create(VocabularyReceiver.class, () -> new VocabularyReceiver(vocabulary));
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @Getter
    public static class ProcessVocabulary implements Serializable {
        private static final long serialVersionUID = -8055752577378492051L;
        private SourceRef<ByteString> source;
        private boolean finalizeStream;
    }

    private final Vocabulary vocabulary;
    private final Materializer materializer;
    private final List<ByteString> chunks = new ArrayList<>();

    private VocabularyReceiver(Vocabulary vocabulary) {
        this.vocabulary = vocabulary;
        this.materializer = ActorMaterializer.create(this.context());
    }

    @Override
    public Receive createReceive() {
        return defaultReceiveBuilder()
                .match(ProcessVocabulary.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(ProcessVocabulary message) {
        this.log().info("Received remote vocabulary source");

        Source<ByteString, NotUsed> source = message.getSource().getSource().watchTermination(this::handleTermination);

        if (message.isFinalizeStream()) {
            source.runWith(Sink.foreach(this::handleVocabularyChunk), this.materializer);
        } else {
            source.alsoTo(Sink.foreach(this::handleVocabularyChunk));
        }
    }

    private void handleVocabularyChunk(ByteString chunk) {
        this.chunks.add(chunk);
    }

    private NotUsed handleTermination(NotUsed notUsed, CompletionStage<Done> stage) {
        stage.thenApply(x -> {
            this.log().info("Done receiving vocabulary");
            return x;
        });
        return notUsed;
    }
}
