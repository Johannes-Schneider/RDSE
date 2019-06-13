package de.hpi.rdse.jujo.training;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class UnencodedSkipGram {

    public static UnencodedSkipGram Empty() {
        return new UnencodedSkipGram("");
    }

    private String expectedOutput;
    private List<String> inputs = new ArrayList<>();

    public UnencodedSkipGram(String expectedOutput) {
        this.expectedOutput = expectedOutput;
    }

    public boolean isEmpty() {
        return this.expectedOutput.isEmpty() || this.inputs.isEmpty();
    }
}
