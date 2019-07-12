package de.hpi.rdse.jujo.training;

import de.hpi.rdse.jujo.fileHandling.FilePartitionIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

public class AliasSampler {

    private static final Logger Log = LogManager.getLogger(FilePartitionIterator.class);

    private final Long[] wordCounts;
    private final double[] s;
    private final int[] a;
    private final Queue<Integer> T_L;
    private final Queue<Integer> T_H;
    private final Random randomGenerator = new Random();
    private final long entireWordCount;

    public AliasSampler(Collection<Long> localWordCounts) {
        Log.info("Start creating AliasSampler");

        this.wordCounts = localWordCounts.toArray(new Long[0]);
        this.entireWordCount = Arrays.stream(this.wordCounts).mapToLong(Long::longValue).sum();
        this.s = this.createS();
        this.a = this.createA();
        this.T_L = this.createT_L();
        this.T_H = this.createT_H();
        this.buildAlias();

        Log.info("Done creating AliasSampler");
    }

    // A=2, B=1, C=0.6, D=0.4
    private double[] createS() {
        double[] s = new double[this.wordCounts.length];

        for (int i = 0; i < this.wordCounts.length; i++) {
            s[i] = this.getWordProbability(i) * this.wordCounts.length;
        }

        return s;
    }

    private double getWordProbability(int i) {
        if (i > this.wordCounts.length - 1) {
            return 0.0f;
        }
        return (double) this.wordCounts[i] / this.entireWordCount;
    }

    // A=0, B=1, C=2, D=3
    private int[] createA() {
        int[] a = new int[this.wordCounts.length];

        for (int i = 0; i < a.length; ++i) {
            a[i] = i;
        }

        return a;
    }

    private Queue<Integer> createT_L() {
        Queue<Integer> T_L = new LinkedList<>();
        for (int i = 0; i < this.wordCounts.length; i++) {
            if (this.getWordProbability(i) < (1.0 / this.wordCounts.length)) {
                T_L.add(i);
            }
        }
        return T_L;
    }

    private Queue<Integer> createT_H() {
        Queue<Integer> T_H = new LinkedList<>();
        for (int i = 0; i < this.wordCounts.length; i++) {
            if (this.getWordProbability(i) > (1.0 / this.wordCounts.length)) {
                T_H.add(i);
            }
        }
        return T_H;
    }

    private void buildAlias() {
        while (this.T_L.size() > 0) {
            int j = this.T_L.poll();
            @SuppressWarnings("ConstantConditions")
            int k = this.T_H.poll();
            this.s[k] = this.s[k] - 1 + this.s[j];
            this.a[j] = k;
            if (this.s[k] < 1.0) {
                this.T_L.add(k);
            } else if (this.s[k] > 1.0) {
                this.T_H.add(k);
            }
            if (T_L.size() > 0 && T_H.size() < 1) {
                this.T_L.forEach(a -> s[a] = 1.0);
                Log.warn(String.format("Corrected rounding errors of %d elements during AliasSampling",
                        this.T_L.size()));
                break;
            }
        }
    }

    public int drawLocalWordIndex() {
        double u = this.randomGenerator.nextDouble() * this.s.length; // u in [0, |W|)
        int uFloor = (int) Math.floor(u);

        if (this.s[uFloor] > u - uFloor) {
            return uFloor;
        }

        return this.a[uFloor];
    }
}
