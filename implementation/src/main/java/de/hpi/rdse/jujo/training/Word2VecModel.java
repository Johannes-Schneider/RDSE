package de.hpi.rdse.jujo.training;

import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import lombok.Getter;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

@Getter
public class Word2VecModel {

    private static Word2VecConfiguration modelConfiguration;

    private static final ReentrantLock instanceLock = new ReentrantLock();
    private static Word2VecModel instance;

    public static Word2VecModel getInstance() {
        Word2VecModel.instanceLock.lock();
        try {
            if (Word2VecModel.instance == null) {
                Word2VecModel.instance = new Word2VecModel();
            }
            return Word2VecModel.instance;
        }
        finally {
            Word2VecModel.instanceLock.unlock();
        }
    }

    public static void setModelConfiguration(Word2VecConfiguration modelConfiguration) {
        Word2VecModel.modelConfiguration = modelConfiguration;
    }

    @Getter
    private final RealVector[] inputWeights;
    @Getter
    private final RealVector[] outputWeights;
    private final Random randomGenerator = new Random();
    @Getter
    private final Word2VecConfiguration configuration;
    @Getter
    private float learningRate;


    private Word2VecModel() {
        this.configuration = Word2VecModel.modelConfiguration.clone();
        this.inputWeights = this.createWeights();
        this.outputWeights = this.createWeights();
        this.learningRate = this.configuration.getLearningRate();
    }

    private RealVector[] createWeights() {
        RealVector[] weights = new RealVector[Vocabulary.getInstance().length()];
        for (int i = 0; i < weights.length; ++i) {
            double[] rawData = this.createRandomWeights(this.configuration.getDimensions());
            weights[i] = new ArrayRealVector(rawData);
        }
        return weights;
    }

    private double[] createRandomWeights(int dimensions) {
        double max = 1.0f / 2.0f * dimensions;
        double[] weights = new double[dimensions];

        for (int i = 0; i < dimensions; ++i) {
            weights[i] = -max + this.randomGenerator.nextDouble() * (max + max);
        }

        return weights;
    }

    public WordEmbedding createEmbedding(String word) {
        long oneHotIndex = Vocabulary.getInstance().oneHotIndex(word);
        int localOneHotIndex = (int) (oneHotIndex - Vocabulary.getInstance().localFirstWordIndex());
        return new WordEmbedding(oneHotIndex, this.inputWeights[localOneHotIndex]);
    }

    public RealVector train(EncodedSkipGram skipGram) {
        Word2VecTrainingStep trainingStep = new Word2VecTrainingStep(skipGram);
        return trainingStep.train();
    }

    public void updateWeight(long oneHotIndex, RealVector gradient) {
        int localIndex = (int) (oneHotIndex - Vocabulary.getInstance().localFirstWordIndex());
        this.inputWeights[localIndex] = this.inputWeights[localIndex].subtract(gradient.mapMultiply(this.learningRate));
    }
}
