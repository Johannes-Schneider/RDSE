package de.hpi.rdse.jujo.training;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.math3.linear.RealVector;

@AllArgsConstructor @Getter @NoArgsConstructor
public class WordEmbedding {

    private long oneHotIndex;
    private RealVector weights;
}
