package de.hpi.rdse.jujo.actors.common;

import akka.Done;
import akka.NotUsed;
import akka.actor.Props;
import akka.actor.RootActorPath;
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
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

public class VocabularyReceiver extends AbstractReapedActor {

    public static Props props() {
        return Props.create(VocabularyReceiver.class, VocabularyReceiver::new);
    }

    @NoArgsConstructor @AllArgsConstructor @Builder @Getter
    public static class ProcessVocabulary implements Serializable {
        private static final long serialVersionUID = -8055752577378492051L;
        private SourceRef<ByteString> source;
        private long vocabularyLength;
    }

    private final Materializer materializer;
    private final Map<RootActorPath, InputStream> remoteStreams = new HashMap<>();

    private VocabularyReceiver() {
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
        this.log().info(String.format("Received remote vocabulary source from %s", this.sender()));
        RootActorPath remote = this.sender().path().root();
        InputStream inputStream = message.getSource().getSource()
                                         .watchTermination((notUsed, stage) -> this.handleTermination(notUsed,
                                                                                                      stage,
                                                                                                      remote,
                                                                                                      message.getVocabularyLength()))
                                         .runWith(StreamConverters.asInputStream(Duration.ofSeconds(3)),
                                                         this.materializer);
        this.remoteStreams.put(remote, inputStream);

    }

    private NotUsed handleTermination(NotUsed notUsed, CompletionStage<Done> stage, RootActorPath remote,
                                      long vocabularyLength) {
        stage.whenComplete((done, throwable) -> {
            if (throwable != null) {
                this.log().error("Exception while receiving vocabulary", throwable);
                return;
            }

            this.log().info(String.format("Done receiving vocabulary from %s", remote));
            try {
                InputStream stream = this.remoteStreams.get(remote);
                BloomFilter<String> bloomFilter = BloomFilter.readFrom(stream, Funnels.stringFunnel(Vocabulary.WORD_ENCODING));
                VocabularyPartition partition = new VocabularyPartition(vocabularyLength,
                                                                        new BloomFilterWordLookupStrategy(bloomFilter));
                Vocabulary.getInstance().addRemoteVocabulary(remote, partition);

                if (Vocabulary.getInstance().isComplete()) {
                    this.log().info("Vocabulary completed. Informing WordEndpoint.");
                    this.context().parent().tell(new WordEndpoint.VocabularyCompleted(), this.self());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return notUsed;
    }
}
