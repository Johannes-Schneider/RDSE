package de.hpi.rdse.jujo.actors.master;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.stream.SinkRef;
import akka.util.ByteString;
import de.hpi.rdse.jujo.actors.AbstractReapedActor;
import de.hpi.rdse.jujo.utils.FilePartition;
import de.hpi.rdse.jujo.utils.FilePartitioner;
import de.hpi.rdse.jujo.utils.startup.MasterCommand;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Master extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "master";

    public static Props props(MasterCommand masterCommand) {
        return Props.create(Master.class, () -> new Master(masterCommand));
    }

    @Getter @Builder @AllArgsConstructor @NoArgsConstructor
    static class SlaveNodeRegistered implements Serializable {
        private static final long serialVersionUID = -1682543505601299772L;
        private ActorRef slave;
    }

    @Getter @Builder @AllArgsConstructor @NoArgsConstructor
    static class SlaveNodeTerminated implements Serializable {
        private static final long serialVersionUID = -3053321777422537935L;
        private ActorRef slave;
    }

    @Getter @NoArgsConstructor @AllArgsConstructor
    static class RequestCorpusPartition implements Serializable {
        private static final long serialVersionUID = 4382410549365244631L;
        private SinkRef<ByteString> sinkRef;
    }

    private final String corpusFile;
    private final FilePartitioner filePartitioner;
    private final ActorRef wordRangeDistributor;

    private Master(MasterCommand masterCommand) {
        // local actor system counts as one slave
        this.corpusFile = masterCommand.getPathToInputFile();
        this.filePartitioner = new FilePartitioner(new File(this.corpusFile), expectedNumberOfSlaves);
        this.self().tell(SlaveNodeRegistered.builder().slave(this.self()).build(), this.self());
        this.wordRangeDistributor = this.createWordRangeDistributor(masterCommand.getNumberOfSlaves(),
                masterCommand.getNumberOfWorkers() > 0);
    }

    private ActorRef createWordRangeDistributor(int expectedNumberOfSlaves, boolean masterContributesWorkers) {
        if (masterContributesWorkers) {
            expectedNumberOfSlaves++;
        }
        return this.context().actorOf(WordRangeDistributor.props(expectedNumberOfSlaves));
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
        this.wordRangeDistributor.tell(message, this.self());
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
