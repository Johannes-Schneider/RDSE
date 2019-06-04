package de.hpi.rdse.jujo.wordManagement;

import akka.actor.ActorRef;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.sun.istack.internal.NotNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.management.OperationsException;
import java.nio.ByteBuffer;
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
    private final WordEndpointResolver wordEndpointResolver;
    private final Map<ActorRef, VocabularyPartition> vocabularyPartitions = new HashMap<>();

    public Vocabulary(String[] words, WordEndpointResolver wordEndpointResolver) {
        this.words = words;
        Arrays.sort(this.words);
        this.wordEndpointResolver = wordEndpointResolver;

        VocabularyPartition localPartition = new VocabularyPartition(this.length(), new LocalWordLookupStrategy(this.words));
        this.vocabularyPartitions.put(this.wordEndpointResolver.localWordEndpoint(), localPartition);
    }

    public long length() {
        return this.words.length;
    }

    @Override @NotNull
    public Iterator<String> iterator() {
        return Arrays.stream(this.words).iterator();
    }

    public void addRemoteVocabulary(ActorRef remoteWordEndpoint, VocabularyPartition vocabulary) {
        this.vocabularyPartitions.putIfAbsent(remoteWordEndpoint, vocabulary);

        if (this.isComplete()) {
            this.initializeVocabularies();
        }
    }

    private void initializeVocabularies() {
        long firstWordIndex = 0L;
        try {
            for (ActorRef responsibleWordEndpoint : this.wordEndpointResolver.all()) {
                this.vocabularyPartitions.get(responsibleWordEndpoint).initialize(firstWordIndex);
                firstWordIndex += this.vocabularyPartitions.get(responsibleWordEndpoint).length();
            }
        } catch (OperationsException e) {
            Log.error("Unable to initialize VocabularyPartition", e);
            this.vocabularyPartitions.clear();
        }
    }

    public boolean isComplete() {
        if (!this.wordEndpointResolver.isReadyToResolve()) {
            return false;
        }

        return this.vocabularyPartitions.keySet().containsAll(this.wordEndpointResolver.all());
    }

    public boolean contains(String word) {
        ActorRef responsibleWordEndpoint = this.wordEndpointResolver.resolve(word);

        if (responsibleWordEndpoint == ActorRef.noSender()) {
            Log.warn("Trying to resolve a word, while WordEndpointResolver is not yet ready");
            responsibleWordEndpoint = this.wordEndpointResolver.localWordEndpoint();
        }

        return this.vocabularyPartitions.get(responsibleWordEndpoint).contains(word);
    }

    public long oneHotIndex(String word) {
        int wordIndex = Arrays.binarySearch(this.words, word);
        if (wordIndex < 0) {
            throw new IllegalArgumentException("The provided word is not part of this local Vocabulary");
        }

        return wordIndex + this.vocabularyPartitions.get(this.wordEndpointResolver.localWordEndpoint()).firstWordIndex();
    }
}
