package de.hpi.rdse.jujo.startup;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import lombok.Getter;

@Getter
@Parameters(commandDescription = "start a slave actor system")
public class SlaveCommand extends CommandBase {

    public static final int DEFAULT_PORT = 7787;

    @Parameter(names = {"--master-host"}, description = "host of the master system", required = true)
    String masterHost;
}
