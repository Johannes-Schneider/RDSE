package de.hpi.rdse.jujo.training;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor @Getter
public class EncodedSkipGram {

    private final String expectedOutput;
    private final WordEmbedding encodedInput;
}
