package de.hpi.rdse.jujo.training;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class WeightVectorTest {

    @Test
    public void testRandomInitialization() {
        int dimensions = 10;
        WeightVector vector = new WeightVector(dimensions);
        float boundary = 1.0f / 2.0f * dimensions;
        Set<Float> uniqueValues = new HashSet<>();

        assertThat(vector.dimensions()).isEqualTo(dimensions);

        for (int i = 0; i < dimensions; ++i) {
            assertThat(vector.get(i)).isBetween(-boundary, boundary);
            uniqueValues.add(vector.get(i));
        }

        assertThat(uniqueValues.size()).isEqualTo(dimensions);
    }

    @Test
    public void testInitialization() {
        float[] weights = { -42.0f, 13.37f, 9000.1f, -6.0f };
        WeightVector vector = new WeightVector(weights);

        assertThat(vector.dimensions()).isEqualTo(weights.length);

        for (int i = 0; i < weights.length; ++i) {
            assertThat(vector.get(i)).isEqualTo(weights[i]);
        }
    }

    @Test
    public void testDotProduct() {
        WeightVector vector1 = new WeightVector(new float[] { -0.5f, 0.5f });
        WeightVector vector2 = new WeightVector(new float[] { 5.0f, 3.0f });

        assertThat(vector1.dotProduct(vector2)).isEqualTo(-1.0f);
        assertThat(vector2.dotProduct(vector1)).isEqualTo(-1.0f);
    }

    @Test
    public void testScale() {
        WeightVector vector = new WeightVector(new float[] { 4.2f, -6.9f });

        assertThat(vector.scale(10.0f)).isEqualTo(new WeightVector(new float[] { 42.0f, -69.0f }));
        assertThat(vector.scale(-10.0f)).isEqualTo(new WeightVector(new float[] { -42.0f, 69.0f }));
    }

    @Test
    public void testAdd() {
        WeightVector vector1 = new WeightVector(new float[] { 5.0f, -25.0f });
        WeightVector vector2 = new WeightVector(new float[] { 7.25f, 13.37f });

        assertThat(vector1.add(vector2)).isEqualTo(new WeightVector(new float[] { 12.25f, -11.63f }));
        assertThat(vector2.add(vector1)).isEqualTo(new WeightVector(new float[] { 12.25f, -11.63f }));
    }

    @Test
    public void testSubtract() {
        WeightVector vector1 = new WeightVector(new float[] { 42.0f, 13.0f });
        WeightVector vector2 = new WeightVector(new float[] { 12.0f, 53.0f });

        assertThat(vector1.subtract(vector2)).isEqualTo(new WeightVector(new float[] { 30.0f, -40.0f }));
        assertThat(vector2.subtract(vector1)).isEqualTo(new WeightVector(new float[] { -30.0f, 40.0f }));
    }
}
