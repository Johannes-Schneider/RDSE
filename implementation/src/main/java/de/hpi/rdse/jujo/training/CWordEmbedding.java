package de.hpi.rdse.jujo.training;


import akka.actor.RootActorPath;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.linear.RealVectorFormat;

import java.io.Serializable;
import java.text.DecimalFormat;

@NoArgsConstructor @AllArgsConstructor @Builder @Getter
public class CWordEmbedding implements Serializable {

    private static final long serialVersionUID = 5837715591126397557L;
    private static final RealVectorFormat cWordFormat = new RealVectorFormat("", "", " ", new DecimalFormat("0" +
            ".##########"));
    private String word;
    private RealVector weights;
    private RootActorPath responsibleTrainer;


    @Override
    public String toString() {
        return String.format("%s {{%s}} %s", word, this.responsibleTrainer, CWordEmbedding.cWordFormat.format(weights));
    }
}
