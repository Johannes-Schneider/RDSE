package de.hpi.rdse.jujo.training;

import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import lombok.Getter;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
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
    private final float learningRate;
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

    public RealVector getInputWeight(int index) {
        return this.inputWeights[index];
    }

    public RealVector getOutputWeight(int index) {
        return this.outputWeights[index];
    }

    public WordEmbedding createInputEmbedding(String word) {
        long globalOneHotIndex = Vocabulary.getInstance().oneHotIndex(word);
        int localOneHotIndex = Vocabulary.getInstance().toLocalOneHotIndex(globalOneHotIndex);
        RealVector inputWeight = this.getInputWeight(localOneHotIndex);

        if (inputWeight.isNaN()) {
            Log.error(String.format("Encountered NAN when creating input embedding! Re-Initializing input weight at " +
                    "%d", localOneHotIndex));
            inputWeight = new ArrayRealVector(this.createRandomWeights(this.configuration.getDimensions()), false);
            this.inputWeights[localOneHotIndex] = inputWeight;
        }

        return new WordEmbedding(globalOneHotIndex, inputWeight);
    }

    public RealVector train(EncodedSkipGram skipGram, int epoch) {
        Word2VecTrainingStep trainingStep = new Word2VecTrainingStep(skipGram, epoch);
        return trainingStep.train();
    }

    public void updateInputWeight(int localOneHotIndex, RealVector gradient, int epoch) {
        if (gradient.isNaN()) {
            Log.error("Encountered NAN when updating input weights!");
            return;
        }
        try {
            this.lockInputWeight(localOneHotIndex);
            RealVector inputWeight = this.getInputWeight(localOneHotIndex);
            RealVector updatedInputWeights = inputWeight.subtract(gradient.mapMultiply(this.getLearningRate(epoch)));

            if (updatedInputWeights.isNaN()) {
                Log.error("Input weights are NAN after performing update!");
                return;
            }

            this.inputWeights[localOneHotIndex] = updatedInputWeights;
        } finally {
            this.unlockInputWeight(localOneHotIndex);
        }
    }

    public void updateOutputWeight(int localOneHotIndex, RealVector gradient, int epoch) {
        if (gradient.isNaN()) {
            Log.error("Encountered NAN when updating output weights!");
            return;
        }
        try {
            this.lockOutputWeight(localOneHotIndex);
            RealVector outputWeight = this.getOutputWeight(localOneHotIndex);
            RealVector updatedOutputWeights = outputWeight.subtract(gradient.mapMultiply(this.getLearningRate(epoch)));

            if (updatedOutputWeights.isNaN()) {
                Log.error("Output weights are NAN after performing update!");
                return;
            }

            this.outputWeights[localOneHotIndex] = updatedOutputWeights;
        } finally {
            this.unlockOutputWeight(localOneHotIndex);
        }
    }

    private void lockInputWeight(int index) {
        this.inputWeightLocks.putIfAbsent(index, new ReentrantLock());
        this.inputWeightLocks.get(index).lock();
    }

    private void unlockInputWeight(int index) {
        ReentrantLock lock = this.inputWeightLocks.get(index);
        if (lock == null) {
            return;
        }

        lock.unlock();
    }

    private void lockOutputWeight(int index) {
        this.outputWeightLocks.putIfAbsent(index, new ReentrantLock());
        this.outputWeightLocks.get(index).lock();
    }

    private void unlockOutputWeight(int index) {
        ReentrantLock lock = this.outputWeightLocks.get(index);
        if (lock == null) {
            return;
        }

        lock.unlock();
    }

    private float getLearningRate(int epoch) {
        Word2VecConfiguration configuration = this.getConfiguration();
        int numberOfEpochs = Math.max(1, configuration.getNumberOfEpochs());
        float delta = (this.learningRate - configuration.getMinimumLearningRate()) / numberOfEpochs;
        float learningRate = this.learningRate - (epoch * delta);

        if (learningRate <= 0.0f) {
            Log.error(String.format("Learning rate <= 0 for epoch %d", epoch));
            return this.learningRate;
        }

        return learningRate;
    }

    public Iterator<CWordEmbedding> getResults() {
        return new Word2VecModelIterator();
    }
}
