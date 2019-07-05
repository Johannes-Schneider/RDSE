package de.hpi.rdse.jujo.training;

import de.hpi.rdse.jujo.fileHandling.FilePartitionIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

public class AliasSampler {

    private static final Logger Log = LogManager.getLogger(FilePartitionIterator.class);

    private Long[] wordCounts;
    private final float[] s;
    private final int[] a;
    private final Queue<Integer> T_L;
    private final Queue<Integer> T_H;
    private final Random randomGenerator = new Random();
    private final long summedWordIdentity;

    public AliasSampler(Collection<Long> localWordCounts) {
        Log.info("Start creating AliasSampler");

        this.wordCounts = localWordCounts.toArray(new Long[0]);
        this.summedWordIdentity = (this.wordCounts.length * (this.wordCounts.length + 1)) / 2;
        this.s = this.createS();
        this.a = this.createA();
        this.T_L = this.createT_L();
        this.T_H = this.createT_H();
        this.buildAlias();

        Log.info("Done creating AliasSampler");
    }

    private float[] createS() {
        float[] s = new float[wordCounts.length];

        for (int i = 0; i < this.wordCounts.length; i++) {
            s[i] = this.getWordProbability(i) * wordCounts.length;
        }

        return s;
    }

    private float getWordProbability(int i) {
        if (i > this.wordCounts.length - 1) {
            return 0.0f;
        }
        return (float) i / this.summedWordIdentity;
    }

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
            int j = this.T_L.peek();
            int k = this.T_H.poll();
            this.s[k] = this.s[k] - 1 + this.s[j];
            this.a[j] = k;
            if (this.s[k] < 1.0) {
                this.T_L.add(k);
            }
            if (this.s[k] > 1.0) {
                this.T_H.add(k);
            }
            this.T_L.poll();
        }
    }

    public int drawLocalWordIndex() {
        float u = this.randomGenerator.nextFloat() * this.s.length; // u in [0, |W|)
        int uFloor = (int) Math.floor(u);

        if (this.s[uFloor] > u - uFloor) {
            return uFloor;
        }

        return this.a[uFloor];
    }
}
