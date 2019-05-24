package de.hpi.rdse.jujo.wordManagement;

import java.util.Map;
import java.util.Random;

public class FrequencyBasedSubsampling implements SubsamplingStrategy {

    private static final double FREQUENCY_CUT_OFF = 10e-5;

    private final long corpusSize;
    private final Map<String, Long> wordCounts;
    private Random random = new Random();

    public FrequencyBasedSubsampling(long corpusSize, Map<String, Long> wordCounts) {
        this.corpusSize = corpusSize;
        this.wordCounts = wordCounts;
    }

    @Override
    public boolean keep(String word) {
        float probability = this.random.nextFloat();
        return Math.sqrt(FREQUENCY_CUT_OFF / this.frequency(word)) > probability;
    }

    private double frequency(String word) {
        return (double) this.wordCounts.get(word) / this.corpusSize;
    }
}
