package de.hpi.rdse.jujo.actors.common;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.StreamConverters;
import akka.util.ByteString;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import de.hpi.rdse.jujo.wordManagement.BloomFilterWordLookupStrategy;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import de.hpi.rdse.jujo.wordManagement.VocabularyPartition;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
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
        private long vocabularyLength;
    }

    private final Vocabulary vocabulary;
    private final Materializer materializer;
    private final Map<ActorRef, BloomFilter<String>> remoteBloomFilters = new HashMap<>();

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

    private void handle(ProcessVocabulary message) throws IOException {
        this.log().info(String.format("Received remote vocabulary source from %s", this.sender()));

        InputStream inputStream = message.getSource().getSource()
                .watchTermination((notUsed, stage) -> this.handleTermination(this.sender(), message.getVocabularyLength(), notUsed, stage))
                .runWith(StreamConverters.asInputStream(), this.materializer);

        this.remoteBloomFilters.put(
                this.sender(),
                BloomFilter.readFrom(inputStream, Funnels.stringFunnel(Vocabulary.WORD_ENCODING)));
    }

    private NotUsed handleTermination(ActorRef sender, long vocabularyLength, NotUsed notUsed, CompletionStage<Done> stage) {
        stage.thenApply(x -> {
            this.log().info(String.format("Done receiving vocabulary from %s", sender));
            VocabularyPartition partition = new VocabularyPartition(vocabularyLength,
                    new BloomFilterWordLookupStrategy(this.remoteBloomFilters.get(sender)));
            this.vocabulary.addRemoteVocabulary(sender, partition);
            return x;
        });
        return notUsed;
    }
}
