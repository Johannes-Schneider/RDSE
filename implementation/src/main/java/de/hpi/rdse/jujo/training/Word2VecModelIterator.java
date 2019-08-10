package de.hpi.rdse.jujo.training;

import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import de.hpi.rdse.jujo.wordManagement.WordEndpointResolver;

import java.util.Iterator;

public class Word2VecModelIterator implements Iterator<CWordEmbedding> {

    private int currentIndex = 0;

    @Override public boolean hasNext() {
        return this.currentIndex < Vocabulary.getInstance().length();
    }

    @Override public CWordEmbedding next() {
        return CWordEmbedding.builder()
                             .word(Vocabulary.getInstance().get(this.currentIndex))
                             .weights(Word2VecModel.getInstance().getInputWeight(this.currentIndex++))
                             .build();
    }
}
