package de.hpi.rdse.jujo.training;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.math3.linear.RealVector;

import java.io.Serializable;

@NoArgsConstructor @AllArgsConstructor @Builder @Getter
public class CWordEmbedding implements Serializable {

    private static final long serialVersionUID = 5837715591126397557L;
    private String word;
    private RealVector weights;

    @Override
    public String toString() {
        return String.format("%s %s", word, weights.toString());
    }
}
