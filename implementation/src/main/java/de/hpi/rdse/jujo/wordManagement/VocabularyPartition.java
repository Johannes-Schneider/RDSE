package de.hpi.rdse.jujo.wordManagement;

import javax.management.OperationsException;

public class VocabularyPartition {

    private long firstWordIndex = -1L;
    private final long length;
    private final WordLookupStrategy wordLookupStrategy;

    public VocabularyPartition(long length, WordLookupStrategy wordLookupStrategy) {
        this.length = length;
        this.wordLookupStrategy = wordLookupStrategy;
    }

    public void initialize(long firstWordIndex) throws OperationsException {
        if (this.firstWordIndex() < 0L) {
            throw new OperationsException("This VocabularyPartition has already been initialized");
        }

        if (firstWordIndex < 0L) {
            throw new IllegalArgumentException("FirstWordIndex must be at least 0");
        }

        this.firstWordIndex = firstWordIndex;
    }

    public long length() {
        return this.length;
    }

    public long firstWordIndex() {
        return this.firstWordIndex;
    }

    public boolean contains(String word) {
        return this.wordLookupStrategy.contains(word);
    }
}
