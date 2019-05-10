package de.hpi.rdse.jujo.utils.startup;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import lombok.Getter;

@Parameters(commandDescription = "start a master actor system")
@Getter
public class MasterCommand extends CommandBase {

    public static final int DEFAULT_PORT = 7789;
    public static final int DEFAULT_DIMENSIONS = 100;

    @Parameter(names = {"-h", "--host"}, description = "host address of this system")
    String host = getDefaultHost();

    @Parameter(names = {"-d", "--dimensions"}, description = "dimensionality of resulting word embeddings")
    int dimensions = DEFAULT_DIMENSIONS;

    @Parameter(names = {"-i", "--input"}, description = "text corpus to train on", validateValueWith = FileValidator.class)
    String pathToInputFile;

    @Parameter(names = {"--slaves"}, description = "number of slaves to wait for")
    int numberOfSlaves;
}
