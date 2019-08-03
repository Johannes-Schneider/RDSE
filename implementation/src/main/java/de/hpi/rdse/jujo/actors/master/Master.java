package de.hpi.rdse.jujo.actors.master;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.common.WorkerCoordinator;
import de.hpi.rdse.jujo.actors.master.training.ResultPartitionReceiver;
import de.hpi.rdse.jujo.startup.MasterCommand;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public class Master extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "master";
    public static Props props(MasterCommand masterCommand) {
        return Props.create(Master.class, () -> new Master(masterCommand));
    }

    @NoArgsConstructor
    public static class AllResultsReceived implements Serializable {
        private static final long serialVersionUID = 2107336118970883673L;
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
        this.workerCoordinator = this.spawnChild(
                WorkerCoordinator.props(masterCommand.getTemporaryWorkingDirectory(),
                        masterCommand.getNumberOfWorkers()));
        this.self().tell(new Shepherd.SlaveNodeRegistrationMessage(this.self()), this.self());
    }

    private ActorRef createWordEndpointDistributor() {
        int expectedNumberOfSlaves = this.masterCommand.getNumberOfSlaves();
        if (this.contributesWorkers) {
            expectedNumberOfSlaves++;
        }
        return this.spawnChild(WordEndpointDistributor.props(expectedNumberOfSlaves));
    }

    private ActorRef createCorpusDistributor() {
        int expectedNumberOfSlaves = this.masterCommand.getNumberOfSlaves();
        if (this.contributesWorkers) {
            expectedNumberOfSlaves++;
        }
        return this.spawnChild(CorpusDistributor.props(expectedNumberOfSlaves, this.masterCommand.getInputFile()));
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(Shepherd.SlaveNodeRegistrationMessage.class, this::handle)
                   .match(ResultPartitionReceiver.ProcessResults.class, this::handle)
                   .match(AllResultsReceived.class, this::handle)
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

        this.resultPartitionReceiver = this.spawnChild(ResultPartitionReceiver.props(this.masterCommand.getOutputFile()));
    }

    private void handle(AllResultsReceived message) {
        this.logProcessStep("Shutdown");

        this.leaveCluster();
        this.purposeHasBeenFulfilled();
    }

    private void leaveCluster() {
        Cluster cluster = Cluster.get(this.context().system());
        cluster.leave(cluster.selfAddress());
    }

    private void redirectToWorkerCoordinator(Object message) {
        this.workerCoordinator.tell(message, this.sender());
    }
}
