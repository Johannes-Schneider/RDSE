package de.hpi.rdse.jujo.training;

import de.hpi.rdse.jujo.fileHandling.FilePartitioner;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import org.apache.commons.math3.analysis.function.Sigmoid;
import org.apache.commons.math3.linear.RealVector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Word2VecTrainingStep {

    private static final Logger Log = LogManager.getLogger(Word2VecTrainingStep.class);

    private final Word2VecModel model = Word2VecModel.getInstance();

    private final EncodedSkipGram skipGram;
    private final WordEmbedding input;
    private final WordEmbedding output;
    private final RealVector outputWordOutputWeights;
    private final Sigmoid sigmoid = new Sigmoid();
    private final int outputLocalIndex;

    private RealVector inputGradient;
    private RealVector outputGradient;


    public Word2VecTrainingStep(EncodedSkipGram skipGram) {
        this.skipGram = skipGram;
        this.input = skipGram.getEncodedInput();
        this.output = this.model.createEmbedding(skipGram.getExpectedOutput());
        this.outputLocalIndex = this.localIndex(this.output.getOneHotIndex());
        this.outputWordOutputWeights = this.model.getOutputWeights()[this.outputLocalIndex];
    }

    private int localIndex(long globalIndex) {
        return (int) (globalIndex - Vocabulary.getInstance().localFirstWordIndex());
    }

    public RealVector train() {
        Log.debug(String.format("Start training for expected output word %s", this.skipGram.getExpectedOutput()));
        double sigmoidResult = this.sigmoid.value(this.outputWordOutputWeights.dotProduct(this.input.getWeights())) - 1;

        this.outputGradient = input.getWeights().mapMultiply(sigmoidResult);
        this.inputGradient = output.getWeights().mapMultiply(sigmoidResult);

        this.trainNegativeSamples();

        this.model.getOutputWeights()[this.outputLocalIndex] =
                this.outputWordOutputWeights.subtract(this.output.getWeights().mapMultiply(this.model.getLearningRate()));
        return this.inputGradient;
    }

    private void trainNegativeSamples() {
        int[] negativeSamples =
                Vocabulary.getInstance().drawLocalSamples(this.model.getConfiguration().getNumberOfNegativeSamples(),
                        this.input.getOneHotIndex(), this.output.getOneHotIndex());

        for (int localSampleIndex : negativeSamples) {
            RealVector sampledWeight = this.model.getOutputWeights()[localSampleIndex];
            double sigmoidResult = this.sigmoid.value(sampledWeight.dotProduct(this.input.getWeights()));
            RealVector sampleGradient = this.input.getWeights().mapMultiply(sigmoidResult);
            this.inputGradient = this.inputGradient.add(sampledWeight.mapMultiply(sigmoidResult));

            this.model.getOutputWeights()[localSampleIndex] =
                    sampledWeight.subtract(sampleGradient.mapMultiply(this.model.getLearningRate()));
        }

    }
}
