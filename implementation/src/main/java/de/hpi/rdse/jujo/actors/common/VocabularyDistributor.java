package de.hpi.rdse.jujo.actors.common;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.SourceRef;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.stream.javadsl.StreamRefs;
import akka.util.ByteString;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import de.hpi.rdse.jujo.wordManagement.WordEndpointResolver;
import lombok.NoArgsConstructor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public class VocabularyDistributor extends AbstractReapedActor {

    private static final double BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.03d;

    public static Props props() {
        return Props.create(VocabularyDistributor.class, VocabularyDistributor::new);
    }

    @NoArgsConstructor
    public static class DistributeVocabulary implements Serializable {
        private static final long serialVersionUID = -126729453919355190L;
    }

    private final Materializer materializer;

    private VocabularyDistributor() {
        this.materializer = ActorMaterializer.create(this.context().system());
    }

    @Override
    public Receive createReceive() {
        return defaultReceiveBuilder()
                .match(DistributeVocabulary.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(DistributeVocabulary message) throws IOException {
        BloomFilter filter = this.createBloomFilter();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        filter.writeTo(outputStream);

        this.log().info(String.format("BloomFilter size = %d bytes", outputStream.size()));
        this.distributeVocabularyToAllEndpoints(outputStream, Vocabulary.getInstance().length());
    }

    private BloomFilter<String> createBloomFilter() {
        this.log().info(String.format("Building BloomFilter for %d words and %f false positive rate",
                                      Vocabulary.getInstance().length(),
                                      BLOOM_FILTER_FALSE_POSITIVE_RATE));
        BloomFilter<String> filter =
                BloomFilter.create(Funnels.stringFunnel(Vocabulary.WORD_ENCODING), Vocabulary.getInstance().length(),
                BLOOM_FILTER_FALSE_POSITIVE_RATE);
        for (String word : Vocabulary.getInstance()) {
            filter.put(word);
        }

        return filter;
    }

    private void distributeVocabularyToAllEndpoints(ByteArrayOutputStream vocabularyStream, long vocabularyLength) {
        this.log().info("About to distribute vocabulary to all WordEndpoints");

        for (ActorRef endpoint : WordEndpointResolver.getInstance().all()) {
            if (endpoint == WordEndpointResolver.getInstance().localWordEndpoint()) {
                continue;
            }

            ByteArrayInputStream inputStream = new ByteArrayInputStream(vocabularyStream.toByteArray());
            Source<ByteString, CompletionStage<IOResult>> source = StreamConverters.fromInputStream(() -> inputStream);
            CompletionStage<SourceRef<ByteString>> sourceRef = source.runWith(StreamRefs.sourceRef(), this.materializer);

            Patterns.pipe(sourceRef.thenApply((ref) -> VocabularyReceiver.ProcessVocabulary.builder()
                                                                                           .source(ref)
                                                                                           .vocabularyLength(vocabularyLength)
                                                                                           .build()), this.context().dispatcher()).to(endpoint, this.self());
        }
    }
}
