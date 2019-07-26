package de.hpi.rdse.jujo.startup;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import de.hpi.rdse.jujo.startup.validators.FileValidator;
import de.hpi.rdse.jujo.startup.validators.ValueGreaterThanZeroValidator;
import lombok.Getter;

import java.nio.file.Path;
import java.nio.file.Paths;

@Parameters(commandDescription = "start a master actor system")
@Getter
public class MasterCommand extends CommandBase {

    private static class StringToPathConverter implements IStringConverter<Path> {
        @Override
        public Path convert(String path) {
            return Paths.get(path);
        }
    }

    public static final int DEFAULT_PORT = 7789;
    public static final int DEFAULT_DIMENSIONS = 100;
    public static final int DEFAULT_WINDOW_SIZE = 3;
    public static final int DEFAULT_NUMBER_OF_EPOCHS = 10;
    public static final float DEFAULT_LEARNING_RATE = 0.1f;
    public static final float DEFAULT_MINIMUM_LEARNING_RATE = 0.0001f;
    public static final int DEFAULT_NUMBER_OF_NEGATIVE_SAMPLES = 5;

    @Parameter(names = {"-i", "--input"}, description = "text corpus to train on", validateValueWith =
            FileValidator.class, required = true, converter = StringToPathConverter.class)
    Path inputFile;

    @Parameter(names = {"--slaves"}, description = "number of slaves to wait for", required = true)
    int numberOfSlaves;

    @Parameter(names = {"-d", "--dimensions"}, description = "dimensionality of resulting word embeddings")
    int dimensions = DEFAULT_DIMENSIONS;

    @Parameter(names = {"--window-size"}, description = "size of window for building skip-grams", validateValueWith =
            ValueGreaterThanZeroValidator.class)
    int windowSize = DEFAULT_WINDOW_SIZE;

    @Parameter(names = {"-e", "--epochs"}, description = "number of epochs to train", validateValueWith =
            ValueGreaterThanZeroValidator.class)
    int numberOfEpochs = DEFAULT_NUMBER_OF_EPOCHS;

    @Parameter(names = {"-l", "--learning-rate"}, description = "initial learning rate", validateValueWith =
            ValueGreaterThanZeroValidator.class)
    float learningRate = DEFAULT_LEARNING_RATE;

    @Parameter(names = {"--min-learning-rate"}, description = "minimum learning rate", validateValueWith =
            ValueGreaterThanZeroValidator.class)
    float minimumLearningRate = DEFAULT_MINIMUM_LEARNING_RATE;

    @Parameter(names = {"-n", "--negative-samples"}, description = "number of negative samples", validateValueWith =
            ValueGreaterThanZeroValidator.class)
    int numberOfNegativeSamples = DEFAULT_NUMBER_OF_NEGATIVE_SAMPLES;

    @Parameter(names = {"-o", "--output"}, description = "path and name to file where result should be stored",
     required = true, converter = StringToPathConverter.class)
    Path outputFile;
}
