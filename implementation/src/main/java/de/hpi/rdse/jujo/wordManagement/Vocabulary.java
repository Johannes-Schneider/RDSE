package de.hpi.rdse.jujo.wordManagement;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.sun.istack.internal.NotNull;
import de.hpi.rdse.jujo.training.AliasSampler;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.management.OperationsException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class Vocabulary implements Iterable<String> {

    private static final Logger Log = LogManager.getLogger(Vocabulary.class);

    public static final Charset WORD_ENCODING = Charset.forName("UTF-8");
    private static final int MIN_NUMBER_OF_HASHING_BITS = 2048;
    private static final HashFunction HASH_FUNCTION = Hashing.goodFastHash(MIN_NUMBER_OF_HASHING_BITS);

    private static Vocabulary instance;

    public static void createInstance(TreeMap<String, Long> subsampledWordCounts) {
        Log.info("Start creating vocabulary");

        if (Vocabulary.instance != null) {
            Log.error("Tried to create a second instance of Vocabulary!");
            return;
        }
        Vocabulary.instance = new Vocabulary(subsampledWordCounts);

        Log.info("Done creating vocabulary");
    }

    public static Vocabulary getInstance() {
        while (Vocabulary.instance == null) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return Vocabulary.instance;
    }

    public static byte[] encode(String phrase) {
        return WORD_ENCODING.encode(phrase).array();
    }

    public static String decode(byte[] encodedPhrase) {
        return decode(encodedPhrase, 0, encodedPhrase.length);
    }

    public static String decode(byte[] encodedPhrase, int offset, int length) {
        return new String(encodedPhrase, offset, length, WORD_ENCODING);
    }

    public static int getByteCount(String phrase) {
        return encode(phrase).length;
    }

    public static HashCode hash(String phrase) {
        return HASH_FUNCTION.hashString(phrase, WORD_ENCODING);
    }

    public static String unify(String word) {
        return word.toLowerCase();
    }

    private final String[] words;
    private final Map<RootActorPath, VocabularyPartition> vocabularyPartitions = new HashMap<>();
    private final AliasSampler aliasSampler;

    private Vocabulary(TreeMap<String, Long> subsampledWordCounts) {
        this.words = subsampledWordCounts.keySet().toArray(new String[0]);
        this.aliasSampler = new AliasSampler(subsampledWordCounts.values());
        VocabularyPartition localPartition = new VocabularyPartition(this.length(), new LocalWordLookupStrategy(this.words));
        this.vocabularyPartitions.put(WordEndpointResolver.getInstance().localWordEndpoint().path().root(), localPartition);
    }

    public int length() {
        return this.words.length;
    }

    public String get(int index) {
        return this.words[index];
    }

    @Override
    public Iterator<String> iterator() {
        return Arrays.stream(this.words).iterator();
    }

    public void addRemoteVocabulary(RootActorPath remote, VocabularyPartition vocabulary) {
        this.vocabularyPartitions.putIfAbsent(remote, vocabulary);

        if (this.isComplete()) {
            this.initializePartitions();
        }
    }

    private void initializePartitions() {
        long firstWordIndex = 0L;
        try {
            for (ActorRef responsibleWordEndpoint : WordEndpointResolver.getInstance().all()) {
                RootActorPath remote = responsibleWordEndpoint.path().root();
                this.vocabularyPartitions.get(remote).initialize(firstWordIndex);
                firstWordIndex += this.vocabularyPartitions.get(remote).length();
            }
        } catch (OperationsException e) {
            Log.error("Unable to initialize VocabularyPartition", e);
            this.vocabularyPartitions.clear();
        }
    }

    public boolean isComplete() {
        if (!WordEndpointResolver.getInstance().isReadyToResolve()) {
            return false;
        }

        List<RootActorPath> allRemotes = WordEndpointResolver.getInstance().all()
                                                             .stream()
                                                             .map(actorRef -> actorRef.path().root())
                                                             .collect(Collectors.toCollection(LinkedList::new));
        return this.vocabularyPartitions.keySet().containsAll(allRemotes);
    }

    public boolean contains(String word) {
        return this.vocabularyPartitions.get(this.getResponsibleWordEndpoint(word)).contains(word);
    }

    public boolean containsLocally(String word) {
        return Arrays.binarySearch(this.words, word) >= 0;
    }

    private RootActorPath getResponsibleWordEndpoint(String word) {
        ActorRef responsibleWordEndpoint = WordEndpointResolver.getInstance().resolve(word);

        if (responsibleWordEndpoint == ActorRef.noSender()) {
            Log.warn("Trying to resolve a word, while WordEndpointResolver is not yet ready");
            responsibleWordEndpoint = WordEndpointResolver.getInstance().localWordEndpoint();
        }

        return responsibleWordEndpoint.path().root();
    }

    public long oneHotIndex(String word) {
        return this.localOneHotIndex(word) + this.localFirstWordIndex();
    }

    public int localOneHotIndex(String word) {
        int wordIndex = Arrays.binarySearch(this.words, word);
        if (wordIndex < 0) {
            throw new IllegalArgumentException("The provided word is not part of this local Vocabulary");
        }

        return wordIndex;
    }

    public long localFirstWordIndex() {
        return this.vocabularyPartitions.get(WordEndpointResolver.getInstance().localWordEndpoint().path().root()).firstWordIndex();
    }

    public int toLocalOneHotIndex(long globalOneHotIndex) {
        return (int) (globalOneHotIndex - Vocabulary.getInstance().localFirstWordIndex());
    }

    public long toGlobalOneHotIndex(int localOneHotIndex) {
        return localOneHotIndex + Vocabulary.getInstance().localFirstWordIndex();
    }

    public int[] drawLocalSamples(int numberOfSamples, long... excludedOneHotIndices) {
        Set<Integer> uniqueLocalIndices = new HashSet<>();
        List<Long> excludedIndices = Arrays.stream(excludedOneHotIndices).boxed().collect(Collectors.toList());
        while (uniqueLocalIndices.size() < numberOfSamples) {
            int localIndex = this.aliasSampler.drawLocalWordIndex();
            if (excludedIndices.contains(this.localFirstWordIndex() + localIndex)) {
                continue;
            }

            uniqueLocalIndices.add(localIndex);
        }
        return ArrayUtils.toPrimitive(uniqueLocalIndices.toArray(new Integer[0]));
    }
}
