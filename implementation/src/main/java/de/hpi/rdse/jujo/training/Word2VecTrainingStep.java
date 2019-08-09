package de.hpi.rdse.jujo.training;

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
    private final RealVector outputWordOutputWeights;
    private final Sigmoid sigmoid = new Sigmoid();
    private final int outputLocalIndex;
    private final int epoch;

    private RealVector inputGradient;
    private RealVector outputGradient;


    public Word2VecTrainingStep(EncodedSkipGram skipGram, int epoch) {
        this.skipGram = skipGram;
        this.input = skipGram.getEncodedInput();
        this.outputLocalIndex = Vocabulary.getInstance().localOneHotIndex(skipGram.getExpectedOutput());
        this.epoch = epoch;
        this.outputWordOutputWeights = this.model.getOutputWeight(this.outputLocalIndex);
    }

    public RealVector train() {
        Log.debug(String.format("Start training for expected output word %s", this.skipGram.getExpectedOutput()));
        if (this.input.getWeights().isNaN()) {
            Log.error("Encountered NAN for input weights during trainingStep");
        }
        if (this.outputWordOutputWeights.isNaN()) {
            Log.error("Encountered NAN for output weights during trainingStep");
        }
        double dotProduct = this.outputWordOutputWeights.dotProduct(this.input.getWeights());
        if (Double.isNaN(dotProduct)) {
            Log.error(String.format("Encountered NAN for dot product during training step using %s and %s",
                    this.outputWordOutputWeights, this.input.getWeights()));
        }
        double sigmoidResult = this.sigmoid.value(dotProduct) - 1;
        if (Double.isNaN(sigmoidResult)) {
            Log.error(String.format("Encountered NAN for sigmoid result of expected output word %s using value %f",
                    this.skipGram.getExpectedOutput(), dotProduct));
        }
        this.outputGradient = this.input.getWeights().mapMultiply(sigmoidResult);
        this.inputGradient = this.outputWordOutputWeights.mapMultiply(sigmoidResult);

        this.trainNegativeSamples();

        this.model.updateOutputWeight(this.outputLocalIndex, this.outputGradient, this.epoch);
        return this.inputGradient;
    }

    private void trainNegativeSamples() {
        int numberOfSamples = this.model.getConfiguration().getNumberOfNegativeSamples();
        long globalOutputIndex = Vocabulary.getInstance().toGlobalOneHotIndex(this.outputLocalIndex);
        int[] negativeSamples = Vocabulary.getInstance().drawLocalSamples(numberOfSamples,
                        this.input.getOneHotIndex(), globalOutputIndex);

        for (int localSampleIndex : negativeSamples) {
            RealVector sampledWeight = this.model.getOutputWeight(localSampleIndex);
            if (sampledWeight.isNaN()) {
                Log.error("Encountered NAN for negative sampled weight");
            }
            double dotProduct = sampledWeight.dotProduct(this.input.getWeights());
            if (Double.isNaN(dotProduct)) {
                Log.error(String.format("Encountered NAN for dot product during negative sampling in training step " +
                                "using %s and %s",
                        sampledWeight, this.input.getWeights()));
            }
            double sigmoidResult = this.sigmoid.value(dotProduct);
            if (Double.isNaN(sigmoidResult)) {
                Log.error(String.format("Encountered NAN for sigmoid result of expected output word %s using value %f",
                        this.skipGram.getExpectedOutput(), dotProduct));
            }
            RealVector sampleGradient = this.input.getWeights().mapMultiply(sigmoidResult);
            this.inputGradient = this.inputGradient.add(sampledWeight.mapMultiply(sigmoidResult));

            this.model.updateOutputWeight(localSampleIndex, sampleGradient, this.epoch);
        }

    }
}
