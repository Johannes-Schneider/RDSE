package de.hpi.rdse.jujo.training;

import de.hpi.rdse.jujo.startup.MasterCommand;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter @NoArgsConstructor @AllArgsConstructor @Builder
public class Word2VecConfiguration  {

    public static Word2VecConfiguration fromMasterCommand(MasterCommand masterCommand) {
        return builder()
                .dimensions(masterCommand.getDimensions())
                .windowSize(masterCommand.getWindowSize())
                .learningRate(masterCommand.getLearningRate())
                .minimumLearningRate(masterCommand.getMinimumLearningRate())
                .numberOfEpochs(masterCommand.getNumberOfEpochs())
                .numberOfNegativeSamples(masterCommand.getNumberOfNegativeSamples())
                .build();
    }

    @Builder.Default
    private int dimensions = MasterCommand.DEFAULT_DIMENSIONS;
    @Builder.Default
    private int windowSize = MasterCommand.DEFAULT_WINDOW_SIZE;
    @Builder.Default
    private float learningRate = MasterCommand.DEFAULT_LEARNING_RATE;
    @Builder.Default
    private float minimumLearningRate = MasterCommand.DEFAULT_MINIMUM_LEARNING_RATE;
    @Builder.Default
    private int numberOfEpochs = MasterCommand.DEFAULT_NUMBER_OF_EPOCHS;
    @Builder.Default
    private int numberOfNegativeSamples = MasterCommand.DEFAULT_NUMBER_OF_NEGATIVE_SAMPLES;

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    public Word2VecConfiguration clone() {
        return builder()
                .dimensions(this.dimensions)
                .windowSize(this.windowSize)
                .learningRate(this.learningRate)
                .minimumLearningRate(this.minimumLearningRate)
                .numberOfEpochs(this.numberOfEpochs)
                .numberOfNegativeSamples(this.numberOfNegativeSamples)
                .build();
    }
}
