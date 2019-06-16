package de.hpi.rdse.jujo.wordManagement;

import akka.actor.ActorRef;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.management.OperationsException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Vocabulary implements Iterable<String> {

    private static final Logger Log = LogManager.getLogger(Vocabulary.class);

    public static final Charset WORD_ENCODING = Charset.forName("UTF-8");
    private static final int MIN_NUMBER_OF_HASHING_BITS = 2048;
    private static final HashFunction HASH_FUNCTION = Hashing.goodFastHash(MIN_NUMBER_OF_HASHING_BITS);

    private static Vocabulary instance;

    public static void createInstance(String[] words) {
        if (Vocabulary.instance != null) {
            Log.error("Tried to create a second instance of Vocabulary!");
            return;
        }
        Vocabulary.instance = new Vocabulary(words);
    }

    public static Vocabulary getInstance() {
        if (Vocabulary.instance == null) {
            Log.error("Called getInstance of Vocabulary without calling createInstance first!");
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
    private final Map<ActorRef, VocabularyPartition> vocabularyPartitions = new HashMap<>();

    private Vocabulary(String[] words) {
        this.words = words;
        Arrays.sort(this.words);

        VocabularyPartition localPartition = new VocabularyPartition(this.length(), new LocalWordLookupStrategy(this.words));
        this.vocabularyPartitions.put(WordEndpointResolver.getInstance().localWordEndpoint(), localPartition);
    }

    public int length() {
        return this.words.length;
    }

    @Override
    public Iterator<String> iterator() {
        return Arrays.stream(this.words).iterator();
    }

    public void addRemoteVocabulary(ActorRef remoteWordEndpoint, VocabularyPartition vocabulary) {
        this.vocabularyPartitions.putIfAbsent(remoteWordEndpoint, vocabulary);

        if (this.isComplete()) {
            this.initializePartitions();
        }
    }

    private void initializePartitions() {
        long firstWordIndex = 0L;
        try {
            for (ActorRef responsibleWordEndpoint : WordEndpointResolver.getInstance().all()) {
                this.vocabularyPartitions.get(responsibleWordEndpoint).initialize(firstWordIndex);
                firstWordIndex += this.vocabularyPartitions.get(responsibleWordEndpoint).length();
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

        return this.vocabularyPartitions.keySet().containsAll(WordEndpointResolver.getInstance().all());
    }

    public boolean contains(String word) {
        return this.vocabularyPartitions.get(this.getResponsibleWordEndpoint(word)).contains(word);
    }

    public boolean containsLocally(String word) {
        return this.getResponsibleWordEndpoint(word) == WordEndpointResolver.getInstance().localWordEndpoint();
    }

    private ActorRef getResponsibleWordEndpoint(String word) {
        ActorRef responsibleWordEndpoint = WordEndpointResolver.getInstance().resolve(word);

        if (responsibleWordEndpoint == ActorRef.noSender()) {
            Log.warn("Trying to resolve a word, while WordEndpointResolver is not yet ready");
            responsibleWordEndpoint = WordEndpointResolver.getInstance().localWordEndpoint();
        }

        return responsibleWordEndpoint;
    }

    public long oneHotIndex(String word) {
        int wordIndex = Arrays.binarySearch(this.words, word);
        if (wordIndex < 0) {
            throw new IllegalArgumentException("The provided word is not part of this local Vocabulary");
        }

        return wordIndex + this.localFirstWordIndex();
    }

    public long localFirstWordIndex() {
        return this.vocabularyPartitions.get(WordEndpointResolver.getInstance().localWordEndpoint()).firstWordIndex();
    }
}
