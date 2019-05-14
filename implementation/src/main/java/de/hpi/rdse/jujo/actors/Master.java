package de.hpi.rdse.jujo.actors;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.stream.SinkRef;
import akka.util.ByteString;
import de.hpi.rdse.jujo.utils.FilePartition;
import de.hpi.rdse.jujo.utils.FilePartitioner;
import de.hpi.rdse.jujo.utils.startup.MasterCommand;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Master extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "master";

    public static Props props(MasterCommand masterCommand) {
        return Props.create(Master.class, () -> new Master(masterCommand));
    }

    @Getter @Builder @AllArgsConstructor @NoArgsConstructor
    static class SlaveNodeRegistered implements Serializable {
        private static final long serialVersionUID = -1682543505601299772L;
        private Address slaveAddress;
        private int numberOfWorkers;

    }

    @Getter @Builder @AllArgsConstructor @NoArgsConstructor
    static class SlaveNodeTerminated implements Serializable {
        private static final long serialVersionUID = -3053321777422537935L;
        private Address slaveAddress;
    }

    @Getter @AllArgsConstructor @NoArgsConstructor
    static class RequestCorpusPartition implements Serializable {
        private static final long serialVersionUID = 4382490549365244631L;
        private SinkRef<ByteString> sinkRef;
    }

    private final Map<Address, Integer> workersPerSlave = new HashMap<>();
    private final Map<ActorRef, ActorRef> corpusSources = new HashMap<>();
    private final String corpusFile;
    private final FilePartitioner filePartitioner;
    private int currentNumberOfSlaves = 0;
    private int numberOfSlavesToStartWork;

    private Master(MasterCommand masterCommand) {
        // local actor system counts as one slave
        this.numberOfSlavesToStartWork = masterCommand.getNumberOfSlaves() + 1;
        this.corpusFile = masterCommand.getPathToInputFile();
        this.filePartitioner = new FilePartitioner(new File(this.corpusFile), numberOfSlavesToStartWork);
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
        if (corpusSources.containsKey(this.sender())) {
            // Slave already requested partition
            return;
        }
        FilePartition filePartition = this.filePartitioner.getNextPartition();
        ActorRef corpusSource = this.getContext().actorOf(CorpusSource.props(new File(this.corpusFile), filePartition));
        this.corpusSources.put(this.sender(), corpusSource);
        corpusSource.tell(CorpusSource.TransferPartition.builder().sinkRef(message.getSinkRef()).build(),
                this.sender());
    }
}
