package de.hpi.rdse.jujo.wordManagement;

import java.util.Map;
import java.util.Random;

public class FrequencyBasedSubsampling implements SubsamplingStrategy {

    private static final double FREQUENCY_CUT_OFF = 1e-3;

    private final Map<String, Long> wordCounts;
    private final Random random = new Random();
    private final int minCount;
    private final double thresholdCount;

    public FrequencyBasedSubsampling(Map<String, Long> wordCounts, int minCount) {
        this.wordCounts = wordCounts;
        this.minCount = minCount;
        this.thresholdCount = this.thresholdCount();
    }

    @Override
    public boolean keep(String word) {
        long wordCount = this.wordCounts.get(word);
        if (wordCount < this.minCount) {
            return false;
        }
        double probability = this.random.nextDouble();
        double wordProbability = (Math.sqrt(wordCount / this.thresholdCount) + 1) * (this.thresholdCount / wordCount);
        wordProbability = Math.min(1, wordProbability);
        return wordProbability > probability;
    }

    private double thresholdCount() {
        long retainCount = wordCounts.values().stream().filter(v -> v >= this.minCount).reduce(0L, Long::sum);
        return retainCount * FREQUENCY_CUT_OFF;
    }
}
