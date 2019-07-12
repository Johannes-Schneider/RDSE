package de.hpi.rdse.jujo.training;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.math3.linear.RealVector;

@NoArgsConstructor @AllArgsConstructor @Builder @Getter
public class CWordEmbedding {

    private String word;
    private RealVector weights;
}
