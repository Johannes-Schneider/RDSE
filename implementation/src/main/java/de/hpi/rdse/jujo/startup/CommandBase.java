package de.hpi.rdse.jujo.startup;

import com.beust.jcommander.Parameter;
import lombok.Getter;

@Getter
abstract class CommandBase {

    private static final int DEFAULT_NUMBER_OF_WORKERS = 4;


    @Parameter(names = {"-h", "--host"}, description = "host address of this system", required = true)
    String host;

    @Parameter(names = {"-w", "--workers"}, description = "number of local workers") final
    int numberOfWorkers = DEFAULT_NUMBER_OF_WORKERS;

    @Parameter(names = {"-t", "--temporary"}, description = "temporary working directory", validateValueWith =
            DirectoryValidator.class, required = true)
    String temporaryWorkingDirectory;

}
