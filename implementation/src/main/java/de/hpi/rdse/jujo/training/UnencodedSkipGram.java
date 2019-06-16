package de.hpi.rdse.jujo.training;

import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Getter
public class UnencodedSkipGram {

    public static UnencodedSkipGram empty() {
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

    public List<EncodedSkipGram> extractEncodedSkipGrams() {
        List<EncodedSkipGram> encodedSkipGrams = new ArrayList<>();
        Iterator<String> inputIterator = this.inputs.iterator();
        while (inputIterator.hasNext()) {
            String word = inputIterator.next();
            if (!Vocabulary.getInstance().containsLocally(word)) {
                continue;
            }
            encodedSkipGrams.add(new EncodedSkipGram(this.expectedOutput, Word2VecModel.getInstance().createEmbedding(word)));
            inputIterator.remove();
        }
        return encodedSkipGrams;
    }
}
