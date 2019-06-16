package de.hpi.rdse.jujo.training;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor @Getter
public class WordEmbedding {

    private long oneHotIndex;
    private float[] weights;
}
