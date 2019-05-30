package de.hpi.rdse.jujo.wordManagement;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;

public class Vocabulary implements Iterable<String> {

    public static final Charset WORD_ENCODING = Charset.forName("UTF-8");
    private static final int MIN_NUMBER_OF_HASHING_BITS = 2048;
    private static final HashFunction HASH_FUNCTION = Hashing.goodFastHash(MIN_NUMBER_OF_HASHING_BITS);


    public static byte[] encode(String phrase) {
        return WORD_ENCODING.encode(phrase).array();
    }

    public static String decode(byte[] encodedPhrase) {
        return decode(ByteBuffer.wrap(encodedPhrase));
    }

    public static String decode(ByteBuffer encodedPhrase) {
        return WORD_ENCODING.decode(encodedPhrase).toString();
    }

    public static int getByteCount(String phrase) {
        return encode(phrase).length;
    }

    public static HashCode hash(String phrase) {
        return HASH_FUNCTION.hashString(phrase, WORD_ENCODING);
    }

    private final String[] words;

    public Vocabulary(String[] words) {
        this.words = words;
    }

    public long length() {
        return this.words.length;
    }

    @Override
    public Iterator<String> iterator() {
        return Arrays.stream(this.words).iterator();
    }
}
