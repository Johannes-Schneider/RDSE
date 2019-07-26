package de.hpi.rdse.jujo.actors.common;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rdse.jujo.startup.ConfigurationWrapper;
import de.hpi.rdse.jujo.wordManagement.FrequencyBasedSubsampling;
import de.hpi.rdse.jujo.wordManagement.SubsamplingStrategy;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import de.hpi.rdse.jujo.wordManagement.WordEndpointResolver;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

public class Subsampler extends AbstractReapedActor {

    public static Props props() {
        return Props.create(Subsampler.class, Subsampler::new);
    }

    @AllArgsConstructor @NoArgsConstructor @Getter
    public static class WordsCounted implements Serializable {
        private static final long serialVersionUID = -5661255174425103187L;
        private Map<String, Long> wordCounts;
    }

    @AllArgsConstructor @NoArgsConstructor @Getter
    public static class TakeOwnershipForWordCounts implements Serializable {
        private static final long serialVersionUID = -5468691953170433564L;
        private Map<String, Long> wordCounts;
        private final String messageId = UUID.randomUUID().toString();
    }

    @AllArgsConstructor @NoArgsConstructor @Getter
    public static class AckOwnership implements Serializable {
        private static final long serialVersionUID = -2180659650723472330L;
        private String messageId;
    }

    @AllArgsConstructor @NoArgsConstructor @Getter
    public static class ConfirmWordOwnershipDistribution implements Serializable {
        private static final long serialVersionUID = -4570745622592953016L;
        private long processedCorpusPartitionSize;
    }

    private final Map<String, Long> wordCounts = new HashMap<>();
    private Map<String, Long> undistributedWordCounts = new HashMap<>();
    private final Set<String> unacknowledgedTakeOverOwnerships = new HashSet<>();
    private final Set<ActorRef> wordEndpointsDoneDistributingWordOwnerships = new HashSet<>();
    private long totalCorpusSize = 0;
    private SubsamplingStrategy subsamplingStrategy;
    private long initialCorpusPartitionSize = 0;

    private Subsampler() {
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(WordsCounted.class, this::handle)
                   .match(TakeOwnershipForWordCounts.class, this::handle)
                   .match(WordEndpoint.WordEndpoints.class, this::handle)
                   .match(AckOwnership.class, this::handle)
                   .match(ConfirmWordOwnershipDistribution.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private void handle(WordsCounted message) {
        this.undistributedWordCounts = message.getWordCounts();
        this.initialCorpusPartitionSize = message.getWordCounts().values().stream().reduce(0L, Long::sum);
        this.log().info(String.format("Number of words in corpus partition: %d being processes on %s",
                this.initialCorpusPartitionSize, this.self().path()));
        if (WordEndpointResolver.getInstance().isReadyToResolve()) {
            this.distributeWordCounts();
        }
    }

    private void distributeWordCounts() {
        Map<ActorRef, Map<String, Long>> mapping = new HashMap<>();
        for (Map.Entry<String, Long> entry : this.undistributedWordCounts.entrySet()) {
            ActorRef target = WordEndpointResolver.getInstance().resolve(entry.getKey());
            mapping.putIfAbsent(target, new HashMap<>());
            mapping.get(target).put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<ActorRef, Map<String, Long>> entry : mapping.entrySet()) {
            this.distributeWordCounts(entry.getKey(), entry.getValue());
        }
    }

    private void distributeWordCounts(ActorRef target, Map<String, Long> wordCounts) {
        Iterator<Map.Entry<String, Long>> it = wordCounts.entrySet().iterator();

        while (it.hasNext()) {
            TakeOwnershipForWordCounts message = this.consumeWordCountsAndCreateMessage(it);
            this.unacknowledgedTakeOverOwnerships.add(message.getMessageId());
            target.tell(message, this.self());

            this.log().debug(String.format("Distributed %d words to %s", message.getWordCounts().size(), target.path()));
        }
    }

    private TakeOwnershipForWordCounts consumeWordCountsAndCreateMessage(Iterator<Map.Entry<String, Long>> iterator) {
        long messageSize = 0;
        long maxMessageSize = (long) (ConfigurationWrapper.getMaximumMessageSize() * 0.9);
        TakeOwnershipForWordCounts message = new TakeOwnershipForWordCounts(new HashMap<>());

        while (iterator.hasNext()) {
            Map.Entry<String, Long> wordCount = iterator.next();
            int wordSize = Vocabulary.getByteCount(wordCount.getKey());
            messageSize += wordSize + Long.BYTES;
            if (messageSize > maxMessageSize) {
                break;
            }

            message.getWordCounts().put(wordCount.getKey(), wordCount.getValue());
            iterator.remove();
        }
        return message;
    }

    private void handle(TakeOwnershipForWordCounts message) {
        this.log().debug(String.format("Taking ownership for %d words from %s", message.getWordCounts().size(),
                this.sender().path()));

        for (String word : message.getWordCounts().keySet()) {
            if (this.wordCounts.computeIfPresent(word,
                    (key, count) -> count + message.getWordCounts().get(word)) == null) {
                this.wordCounts.put(word, message.getWordCounts().get(word));
            }
        }
        this.sender().tell(new AckOwnership(message.getMessageId()), this.self());
    }

    private void handle(WordEndpoint.WordEndpoints message) {
        if (this.undistributedWordCounts.isEmpty()) {
            return;
        }
        this.distributeWordCounts();
    }

    private void handle(AckOwnership message) {
        this.unacknowledgedTakeOverOwnerships.remove(message.getMessageId());
        if (this.unacknowledgedTakeOverOwnerships.isEmpty()) {
            WordEndpointResolver.getInstance().broadcast(new ConfirmWordOwnershipDistribution(this.initialCorpusPartitionSize), this.self());
        }
    }

    private void handle(ConfirmWordOwnershipDistribution message) {
        if (!this.wordEndpointsDoneDistributingWordOwnerships.add(this.sender())) {
            return;
        }
        this.totalCorpusSize += message.getProcessedCorpusPartitionSize();
        if (this.wordEndpointsDoneDistributingWordOwnerships.size() < WordEndpointResolver.getInstance().all().size()) {
            return;
        }
        this.subsample();
    }

    private void subsample() {
        this.log().info(String.format("Starting sub-sampling (total number of words = %d; local number of words = %d)",
                this.totalCorpusSize, this.wordCounts.values().stream().reduce(0L, Long::sum)));

        this.subsamplingStrategy = new FrequencyBasedSubsampling(this.totalCorpusSize, this.wordCounts);
        TreeMap<String, Long> uniqueWords = new TreeMap<>();
        for (Map.Entry<String, Long> wordCount : this.wordCounts.entrySet()) {
            if (this.subsamplingStrategy.keep(wordCount.getKey())) {
                uniqueWords.put(wordCount.getKey(), wordCount.getValue());
            } else {
                this.log().debug(String.format("Subsampling discards word %s", wordCount.getKey()));
            }
        }

        this.log().info(String.format("Done sub-sampling; kept %f %% (%d) unique words",
                uniqueWords.size() / (double) this.wordCounts.size() * 100, uniqueWords.size()));

        Vocabulary.createInstance(uniqueWords);
        WordEndpointResolver.getInstance().localWordEndpoint().tell(new WordEndpoint.VocabularyCreated(), this.self());
    }
}
