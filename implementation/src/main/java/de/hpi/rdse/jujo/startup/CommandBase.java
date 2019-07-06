package de.hpi.rdse.jujo.startup;

import com.beust.jcommander.Parameter;
import lombok.Getter;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Getter
abstract class CommandBase {

    private static final int DEFAULT_NUMBER_OF_WORKERS = 4;

    @Parameter(names = {"-w", "--workers"}, description = "number of local workers")
    int numberOfWorkers = DEFAULT_NUMBER_OF_WORKERS;
}
