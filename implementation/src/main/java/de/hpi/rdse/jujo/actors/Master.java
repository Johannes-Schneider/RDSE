package de.hpi.rdse.jujo.actors;

import akka.actor.Address;
import akka.actor.Props;
import de.hpi.rdse.jujo.utils.startup.MasterCommand;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.io.Serializable;

public class Master extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "master";

    public static Props props(MasterCommand masterCommand) {
        return Props.create(Master.class, () -> new Master(masterCommand));
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    static class SlaveNodeRegistrationMessage implements Serializable {
        private static final long serialVersionUID = -1682543505601299772L;
        private Address slaveAddress;
        private int numberOfWorkers;

    }
    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    static class SlaveNodeTerminatedMessage implements Serializable {
        private static final long serialVersionUID = -3053321777422537935L;
        private Address slaveAddress;

    }
    private int currentNumberOfSlaves = 0;
    private int numberOfSlavesToStartWork;

    private Master(MasterCommand masterCommand) {
        try {
            this.parseInputFile(masterCommand.getPathToInputFile());
        } catch (IOException e) {
            this.log().error(e, "Error while processing input file.");
        }
        // local actor system counts as one slave
        this.numberOfSlavesToStartWork = masterCommand.getNumberOfSlaves() + 1;
        this.self().tell(SlaveNodeRegistrationMessage.builder()
                        .slaveAddress(this.self().path().address())
                        .numberOfWorkers(masterCommand.getNumberOfWorkers())
                        .build(),
                this.self()
        );
    }

    private void parseInputFile(String pathToInputFile) throws IOException {

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchAny(this::handleAny)
                .build();
    }
}
