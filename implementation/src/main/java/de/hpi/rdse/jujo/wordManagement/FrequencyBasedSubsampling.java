package de.hpi.rdse.jujo.wordManagement;

import java.util.Map;
import java.util.Random;

public class FrequencyBasedSubsampling implements SubsamplingStrategy {

    private static final double FREQUENCY_CUT_OFF = 1e-5;

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
        double frequency = this.frequency(word);
        double precedingFraction = (frequency - FREQUENCY_CUT_OFF) / frequency;
        return (precedingFraction - Math.sqrt(FREQUENCY_CUT_OFF / frequency)) > probability;
    }

    private double frequency(String word) {
        return (double) this.wordCounts.get(word) / this.corpusSize;
    }
}
