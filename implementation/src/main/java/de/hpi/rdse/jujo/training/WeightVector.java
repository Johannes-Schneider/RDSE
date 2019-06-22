package de.hpi.rdse.jujo.training;

import java.util.Random;

public class WeightVector {

    private final float[] weights;

    public WeightVector(int dimensions) {
        this.weights = createRandomWeights(dimensions);
    }

    public WeightVector(float[] weights) {
        this.weights = weights;
    }

    private float[] createRandomWeights(int dimensions) {
        Random rnd = new Random();
        float max = 1.0f / 2.0f * dimensions;
        float[] weights = new float[dimensions];

        for (int i = 0; i < dimensions; ++i) {
            weights[i] = -max + rnd.nextFloat() * (max + max);
        }

        return weights;
    }

    public int dimensions() {
        return weights.length;
    }

    public float get(int dimension) {
        return this.weights[dimension];
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof WeightVector)) {
            return false;
        }

        WeightVector other = (WeightVector) obj;
        if (this.dimensions() != other.dimensions()) {
            return false;
        }

        for (int i = 0; i < this.dimensions(); ++i) {
            if (this.get(i) != other.get(i)) {
                return false;
            }
        }

        return true;
    }

    public float dotProduct(WeightVector other) {
        assert this.dimensions() == other.dimensions();

        float result = 0.0f;
        for (int i = 0; i < this.dimensions(); ++i) {
            result += this.get(i) * other.get(i);
        }

        return result;
    }

    public WeightVector scale(float factor) {
        float[] weights = new float[this.dimensions()];
        for (int i = 0; i < this.dimensions(); ++i) {
            weights[i] = this.get(i) * factor;
        }

        return new WeightVector(weights);
    }

    public WeightVector add(WeightVector other) {
        assert this.dimensions() == other.dimensions();
        float[] weights = new float[this.dimensions()];

        for (int i = 0; i < this.dimensions(); ++i) {
            weights[i] = this.get(i) + other.get(i);
        }

        return new WeightVector(weights);
    }

    public WeightVector subtract(WeightVector other) {
        assert this.dimensions() == other.dimensions();
        float[] weights = new float[this.dimensions()];

        for (int i = 0; i < this.dimensions(); ++i) {
            weights[i] = this.get(i) - other.get(i);
        }

        return new WeightVector(weights);
    }
}
