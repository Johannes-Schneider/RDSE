package de.hpi.rdse.jujo.training;

import de.hpi.rdse.jujo.startup.MasterCommand;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter @AllArgsConstructor @NoArgsConstructor
public class Word2VecConfiguration  {

    public static Word2VecConfiguration fromMasterCommand(MasterCommand masterCommand) {
        return new Word2VecConfiguration(masterCommand.getDimensions(), masterCommand.getWindowSize(),
                masterCommand.getLearningRate(), masterCommand.getNumberOfEpochs(),
                masterCommand.getNumberOfNegativeSamples());
    }

    private int dimensions = MasterCommand.DEFAULT_DIMENSIONS;
    private int windowSize = MasterCommand.DEFAULT_WINDOW_SIZE;
    private float learningRate = MasterCommand.DEFAULT_LEARNING_RATE;
    private int numberOfEpochs = MasterCommand.DEFAULT_NUMBER_OF_EPOCHS;
    private int numberOfNegativeSamples = MasterCommand.DEFAULT_NUMBER_OF_NEGATIVE_SAMPLES;

    public Word2VecConfiguration clone() {
        return new Word2VecConfiguration(this.dimensions, this.windowSize, this.learningRate, this.numberOfEpochs,
                this.numberOfNegativeSamples);
    }
}
