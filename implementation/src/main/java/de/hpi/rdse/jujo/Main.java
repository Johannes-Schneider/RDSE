package de.hpi.rdse.jujo;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import de.hpi.rdse.jujo.utils.startup.MasterCommand;
import de.hpi.rdse.jujo.utils.startup.SlaveCommand;

public class Main {

    public static void main(String[] args) {

        // Parse the command-line args.
        MasterCommand masterCommand = new MasterCommand();
        SlaveCommand slaveCommand = new SlaveCommand();
        JCommander jCommander = JCommander.newBuilder()
                .addCommand("master", masterCommand)
                .addCommand("slave", slaveCommand)
                .build();

        try {
            jCommander.parse(args);

            if (jCommander.getParsedCommand() == null) {
                throw new ParameterException("No command given.");
            }

            // Start a master or slave.
            switch (jCommander.getParsedCommand()) {
                case "master":
                    startMaster(masterCommand);
                    break;
                case "slave":
                    startSlave(slaveCommand);
                    break;
                default:
                    throw new AssertionError();

            }

        } catch (ParameterException e) {
            System.out.printf("Could not parse args: %s\n", e.getMessage());
            if (jCommander.getParsedCommand() == null) {
                jCommander.usage();
            } else {
                jCommander.usage(jCommander.getParsedCommand());
            }
            System.exit(1);
        }

    }

    private static void startMaster(MasterCommand masterCommand) {
        Bootstrap.runMaster(masterCommand);
    }

    private static void startSlave(SlaveCommand slaveCommand) {
        Bootstrap.runSlave(slaveCommand);
    }
}