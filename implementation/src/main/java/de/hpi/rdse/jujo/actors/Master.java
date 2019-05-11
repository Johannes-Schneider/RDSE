package de.hpi.rdse.jujo.actors;

import akka.actor.Address;
import akka.actor.Props;
import akka.stream.SinkRef;
import de.hpi.rdse.jujo.utils.FilePartitioner;
import de.hpi.rdse.jujo.utils.startup.MasterCommand;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Master extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "master";

    public static Props props(MasterCommand masterCommand) {
        return Props.create(Master.class, () -> new Master(masterCommand));
    }

    @Getter
    @Builder
    @AllArgsConstructor
    static class SlaveNodeRegistered implements Serializable {
        private static final long serialVersionUID = -1682543505601299772L;
        private final Address slaveAddress;
        private final int numberOfWorkers;

    }

    @Getter
    @Builder
    @AllArgsConstructor
    static class SlaveNodeTerminated implements Serializable {
        private static final long serialVersionUID = -3053321777422537935L;
        private final Address slaveAddress;
    }

    @Getter
    @Builder
    @AllArgsConstructor
    public static class RequestCorpusPartition implements Serializable {
        private static final long serialVersionUID = -683972376493023381L;
        private final SinkRef<String> sink;
    }

    private final Map<Address, Integer> workersPerSlave = new HashMap<>();
    private final FilePartitioner filePartitioner;
    private int currentNumberOfSlaves = 0;
    private int numberOfSlavesToStartWork;

    private Master(MasterCommand masterCommand) {
        // local actor system counts as one slave
        numberOfSlavesToStartWork = masterCommand.getNumberOfSlaves() + 1;
        filePartitioner = new FilePartitioner(new File(masterCommand.getPathToInputFile()), numberOfSlavesToStartWork);
        this.self().tell(SlaveNodeRegistered.builder()
                        .slaveAddress(this.self().path().address())
                        .numberOfWorkers(masterCommand.getNumberOfWorkers())
                        .build(),
                this.self()
        );
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SlaveNodeRegistered.class, this::handle)
                .match(RequestCorpusPartition.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(SlaveNodeRegistered message) {
        if (this.workersPerSlave.putIfAbsent(message.getSlaveAddress(), message.getNumberOfWorkers()) != null) {
            return;
        }

        currentNumberOfSlaves++;
        if (currentNumberOfSlaves == numberOfSlavesToStartWork) {
            // TODO: start working
        }
    }

    private void handle(RequestCorpusPartition message) {

    }
}
