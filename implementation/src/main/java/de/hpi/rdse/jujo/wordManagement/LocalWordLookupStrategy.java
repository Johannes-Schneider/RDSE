package de.hpi.rdse.jujo.wordManagement;

import java.util.Arrays;

public class LocalWordLookupStrategy implements WordLookupStrategy {

    private final String[] localWords;

    public LocalWordLookupStrategy(String[] localWords) {
        this.localWords = localWords;
    }

    @Override
    public boolean contains(String word) {
        return Arrays.binarySearch(this.localWords, word) >= 0;
    }
}
