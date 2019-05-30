package de.hpi.rdse.jujo.wordManagement;

import com.google.common.hash.BloomFilter;

public class BloomFilterWordLookupStrategy implements WordLookupStrategy {

    private final BloomFilter<String> bloomFilter;

    public BloomFilterWordLookupStrategy(BloomFilter<String> bloomFilter) {
        this.bloomFilter = bloomFilter;
    }

    @Override
    public boolean contains(String word) {
        return bloomFilter.mightContain(word);
    }
}
