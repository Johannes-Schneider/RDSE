package de.hpi.rdse.jujo.training;

import java.util.Collection;
import java.util.Random;

public class AliasSampler {

    private final float[] s;
    private final int[] a;
    private final Random randomGenerator = new Random();

    public AliasSampler(Collection<Long> localWordCounts) {
        this.s = this.createS(localWordCounts);
        this.a = this.createA(localWordCounts);
        this.buildAlias();
    }

    private float[] createS(Collection<Long> localWordCounts) {
        long totalNumberOfWords = localWordCounts.stream().reduce(0L, Long::sum);
        float[] s = new float[localWordCounts.size()];

        int i = 0;
        for (Long wordCount : localWordCounts) {
            float frequency = (float) wordCount / totalNumberOfWords;
            s[i] = frequency * localWordCounts.size();
            i++;
        }

        return s;
    }

    private int[] createA(Collection<Long> localWordCounts) {
        int[] a = new int[localWordCounts.size()];

        for (int i = 0; i < a.length; ++i) {
            a[i] = i;
        }

        return a;
    }

    private void buildAlias() {
        for (int j = 0; j < this.s.length; ++j) {
            if (this.s[j] >= 1.0f) {
                continue;
            }

            for (int k = 0; k < this.s.length; ++k) {
                if (this.s[k] <= 1.0f) {
                    continue;
                }

                this.a[j] = k;
                this.s[k] = this.s[k] - 1.0f + this.s[j];
            }
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
