package de.hpi.rdse.jujo.training;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor @Getter @NoArgsConstructor
public class EncodedSkipGram {

    private String expectedOutput;
    private WordEmbedding encodedInput;
}
