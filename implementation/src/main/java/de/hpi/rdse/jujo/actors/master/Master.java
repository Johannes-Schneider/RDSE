package de.hpi.rdse.jujo.actors.master;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.cluster.metrics.ClusterMetricsChanged;
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
    private final ActorRef metricsReceiver;
    private final boolean contributesWorkers;
    private final ActorRef workerCoordinator;
    private ActorRef resultPartitionReceiver;
    private boolean workerCoordinatorTerminated = false;
    private boolean resultsReceived = false;

    private Master(MasterCommand masterCommand) {
        this.masterCommand = masterCommand;
        this.contributesWorkers = masterCommand.getNumberOfWorkers() > 0;
        this.wordEndpointDistributor = this.createWordEndpointDistributor();
        this.corpusDistributor = this.createCorpusDistributor();
        this.workerCoordinator = this.context().actorOf(
                WorkerCoordinator.props(masterCommand.getTemporaryWorkingDirectory(),
                        masterCommand.getNumberOfWorkers()));
        this.context().watch(this.workerCoordinator);
        this.self().tell(new Shepherd.SlaveNodeRegistrationMessage(this.self()), this.self());
        this.metricsReceiver = this.context().actorOf(MetricsReceiver.props());
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
                   .match(ClusterMetricsChanged.class, this::handle)
                   .match(AllResultsReceived.class, this::handle)
                   .match(Terminated.class, this::handle)
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

    private void handle(AllResultsReceived message) {
        this.sender().tell(PoisonPill.getInstance(), ActorRef.noSender());
        this.resultsReceived = true;
        this.terminate();
    }

    private void redirectToWorkerCoordinator(Object message) {
        this.workerCoordinator.tell(message, this.sender());
    }

    private void handle(Terminated message) {
        if (message.actor() == this.workerCoordinator) {
            this.workerCoordinatorTerminated = true;
        }
        this.terminate();
    }

    private void terminate() {
        if (this.workerCoordinatorTerminated && this.resultsReceived) {
            this.log().info("All work has been done. Goodbye!");
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
    }

    private void handle(ClusterMetricsChanged message) {
        this.metricsReceiver.tell(message, this.sender());
    }
}
