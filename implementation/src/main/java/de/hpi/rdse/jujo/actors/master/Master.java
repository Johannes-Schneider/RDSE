package de.hpi.rdse.jujo.actors.master;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.common.WorkerCoordinator;
import de.hpi.rdse.jujo.actors.master.training.ResultPartitionReceiver;
import de.hpi.rdse.jujo.startup.MasterCommand;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public class Master extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "master";

    public static Props props(MasterCommand masterCommand) {
        return Props.create(Master.class, () -> new Master(masterCommand));
    }

    @Getter @Builder @AllArgsConstructor @NoArgsConstructor
    static class SlaveNodeTerminated implements Serializable {
        private static final long serialVersionUID = -3053321777422537935L;
        private ActorRef slave;
    }

    private final MasterCommand masterCommand;
    private final ActorRef wordEndpointDistributor;
    private final ActorRef corpusDistributor;
    private final boolean contributesWorkers;
    private final ActorRef workerCoordinator;
    private ActorRef resultPartitionReceiver;

    private Master(MasterCommand masterCommand) {
        this.masterCommand = masterCommand;
        this.contributesWorkers = masterCommand.getNumberOfWorkers() > 0;
        this.wordEndpointDistributor = this.createWordEndpointDistributor();
        this.corpusDistributor = this.createCorpusDistributor();
        this.workerCoordinator = this.context().actorOf(
                WorkerCoordinator.props(masterCommand.getTemporaryWorkingDirectory(),
                        masterCommand.getNumberOfWorkers()));
        this.self().tell(new Shepherd.SlaveNodeRegistrationMessage(this.self()), this.self());
    }

    private ActorRef createWordEndpointDistributor() {
        int expectedNumberOfSlaves = this.masterCommand.getNumberOfSlaves();
        if (this.contributesWorkers) {
            expectedNumberOfSlaves++;
        }
        return this.context().actorOf(WordEndpointDistributor.props(expectedNumberOfSlaves));
    }

    private ActorRef createCorpusDistributor() {
        int expectedNumberOfSlaves = this.masterCommand.getNumberOfSlaves();
        if (this.contributesWorkers) {
            expectedNumberOfSlaves++;
        }
        return this.context().actorOf(
                CorpusDistributor.props(expectedNumberOfSlaves, this.masterCommand.getInputFile()));
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(Shepherd.SlaveNodeRegistrationMessage.class, this::handle)
                   .match(ResultPartitionReceiver.ProcessResults.class, this::handle)
                   .matchAny(this::redirectToWorkerCoordinator)
                   .build();
    }

    private void handle(Shepherd.SlaveNodeRegistrationMessage message) {
        this.wordEndpointDistributor.tell(message, this.self());
        this.corpusDistributor.tell(message, this.self());
    }

    private void handle(ResultPartitionReceiver.ProcessResults message) {
        this.initializeResultPartitionReceiver();

        this.resultPartitionReceiver.tell(message, this.sender());
    }

    private void initializeResultPartitionReceiver() {
        if (this.resultPartitionReceiver != null) {
            return;
        }

        this.resultPartitionReceiver = this.context().actorOf(ResultPartitionReceiver.props(this.masterCommand.getOutputFile()));
    }

    private void redirectToWorkerCoordinator(Object message) {
        this.workerCoordinator.tell(message, this.sender());
    }
}
