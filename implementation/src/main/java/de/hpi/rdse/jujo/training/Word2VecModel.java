package de.hpi.rdse.jujo.training;

import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import lombok.Getter;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class Word2VecModel {

    private static final Logger Log = LogManager.getLogger(Word2VecModel.class);

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
        } finally {
            Word2VecModel.instanceLock.unlock();
        }
    }

    public static void setModelConfiguration(Word2VecConfiguration modelConfiguration) {
        Word2VecModel.modelConfiguration = modelConfiguration;
    }

    private final RealVector[] inputWeights;
    private final RealVector[] outputWeights;
    private final Random randomGenerator = new Random();
    @Getter
    private final Word2VecConfiguration configuration;
    @Getter
    private float learningRate;
    private final ConcurrentHashMap<Integer, ReentrantLock> inputWeightLocks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ReentrantLock> outputWeightLocks = new ConcurrentHashMap<>();


    private Word2VecModel() {
        Log.info("Start initializing Word2VecModel");

        this.configuration = Word2VecModel.modelConfiguration.clone();
        this.inputWeights = this.createWeights();
        this.outputWeights = this.createWeights();
        this.learningRate = this.configuration.getLearningRate();

        Log.info("Done initializing Word2VecModel");
    }

    public void setInputWeight(int index, RealVector newWeight) {
        this.inputWeightLocks.putIfAbsent(index, new ReentrantLock());
        this.inputWeightLocks.get(index).lock();
        try {
            this.inputWeights[index] = newWeight;
        } finally {
            this.inputWeightLocks.get(index).unlock();
            this.inputWeightLocks.remove(index);
        }
    }

    public void setOutputWeight(int index, RealVector newWeight) {
        this.outputWeightLocks.putIfAbsent(index, new ReentrantLock());
        this.outputWeightLocks.get(index).lock();
        try {
            this.outputWeights[index] = newWeight;
        } finally {
            this.outputWeightLocks.get(index).unlock();
            this.outputWeightLocks.remove(index);
        }
    }

    public RealVector getInputWeight(int index) {
        this.inputWeightLocks.putIfAbsent(index, new ReentrantLock());
        this.inputWeightLocks.get(index).lock();
        return this.inputWeights[index];
    }

    public RealVector getOutputWeight(int index) {
        this.outputWeightLocks.putIfAbsent(index, new ReentrantLock());
        this.outputWeightLocks.get(index).lock();
        return this.outputWeights[index];
    }

    private RealVector[] createWeights() {
        RealVector[] weights = new RealVector[Vocabulary.getInstance().length()];
        for (int i = 0; i < weights.length; ++i) {
            double[] rawData = this.createRandomWeights(this.configuration.getDimensions());
            weights[i] = new ArrayRealVector(rawData, false);
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
        try {
            RealVector inputWeight = this.getInputWeight(localOneHotIndex);
            return new WordEmbedding(oneHotIndex, inputWeight);
        } finally {
            this.inputWeightLocks.get(localOneHotIndex).unlock();
            this.inputWeightLocks.remove(localOneHotIndex);
        }
    }

    public RealVector train(EncodedSkipGram skipGram) {
        Word2VecTrainingStep trainingStep = new Word2VecTrainingStep(skipGram);
        return trainingStep.train();
    }

    public void updateWeight(long oneHotIndex, RealVector gradient) {
        int localIndex = (int) (oneHotIndex - Vocabulary.getInstance().localFirstWordIndex());
        RealVector inputWeight = this.getInputWeight(localIndex);
        this.setInputWeight(localIndex, inputWeight.subtract(gradient.mapMultiply(this.learningRate)));
    }
}
