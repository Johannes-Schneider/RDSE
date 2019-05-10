package de.hpi.rdse.jujo.utils.startup;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import lombok.Getter;

@Getter
@Parameters(commandDescription = "start a slave actor system")
public class SlaveCommand extends CommandBase {

    public static final int DEFAULT_PORT = 7787;

    @Parameter(names = {"-h", "--host"}, description = "host of the master system")
    String masterHost;
}
