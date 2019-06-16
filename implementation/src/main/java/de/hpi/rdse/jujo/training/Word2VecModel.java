package de.hpi.rdse.jujo.training;

import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import lombok.Getter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Random;

@Getter
public class Word2VecModel {

    private static final Random randomGenerator = new Random();
    private static Word2VecConfiguration modelConfiguration;

    private static Word2VecModel instance;

    public static Word2VecModel getInstance() {
        if (Word2VecModel.instance == null) {
            Word2VecModel.instance = new Word2VecModel();
        }
        return Word2VecModel.instance;
    }

    public static void setModelConfiguration(Word2VecConfiguration modelConfiguration) {
        Word2VecModel.modelConfiguration = modelConfiguration;
    }

    private static float generateRandomWeight(int dimensions) {
        float boundary = (1.0f/2 * dimensions);
        return  -boundary + randomGenerator.nextFloat() * (boundary + boundary);
    }
    
    private final float[][] weights;
    private final Word2VecConfiguration configuration;

    private Word2VecModel() {
        this.configuration = Word2VecModel.modelConfiguration.clone();
        this.weights = this.createWeights();
    }

    private float[][] createWeights() {
        float[][] weights = new float[Vocabulary.getInstance().length()][this.configuration.getDimensions()];
        for (int wordIndex = 0; wordIndex < Vocabulary.getInstance().length(); ++wordIndex) {
            for (int weightIndex = 0; weightIndex < this.configuration.getDimensions(); ++weightIndex) {
                weights[wordIndex][weightIndex] = generateRandomWeight(this.configuration.getDimensions());
            }
        }
        return weights;
    }

    public WordEmbedding createEmbedding(String word) {
        long oneHotIndex = Vocabulary.getInstance().oneHotIndex(word);
        int localOneHotIndex = (int) (oneHotIndex - Vocabulary.getInstance().localFirstWordIndex());
        return new WordEmbedding(oneHotIndex, this.weights[localOneHotIndex]);
    }

    public void train(EncodedSkipGram skipGram) {
        // TODO: Implement actual training
        throw new NotImplementedException();
    }
}
