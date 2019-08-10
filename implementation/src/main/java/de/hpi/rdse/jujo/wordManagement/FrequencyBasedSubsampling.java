package de.hpi.rdse.jujo.wordManagement;

import java.util.Map;
import java.util.Random;

public class FrequencyBasedSubsampling implements SubsamplingStrategy {

    private static final double FREQUENCY_CUT_OFF = 1e-3;

    private final long corpusSize;
    private final Map<String, Long> wordCounts;
    private final Random random = new Random();
    private final int minCount;

    public FrequencyBasedSubsampling(long corpusSize, Map<String, Long> wordCounts, int minCount) {
        this.corpusSize = corpusSize;
        this.wordCounts = wordCounts;
        this.minCount = minCount;
    }

    @Override
    public boolean keep(String word) {
        if (this.wordCounts.get(word) < this.minCount) {
            return false;
        }
        float probability = this.random.nextFloat();
        return Math.sqrt(FREQUENCY_CUT_OFF / this.frequency(word)) > probability;
    }

    private double frequency(String word) {
        return (double) this.wordCounts.get(word) / this.corpusSize;
    }
}
