package de.hpi.rdse.jujo.actors.master;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.common.WorkerCoordinator;
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

    private final ActorRef wordEndpointDistributor;
    private final ActorRef corpusDistributor;
    private final boolean contributesWorkers;
    private final ActorRef workerCoordinator;

    private Master(MasterCommand masterCommand) {
        this.contributesWorkers = masterCommand.getNumberOfWorkers() > 0;
        this.wordEndpointDistributor = this.createWordEndpointDistributor(masterCommand);
        this.corpusDistributor = this.createCorpusDistributor(masterCommand);
        this.workerCoordinator = this.context().actorOf(
                WorkerCoordinator.props(masterCommand.getTemporaryWorkingDirectory()));
        this.self().tell(new Shepherd.SlaveNodeRegistrationMessage(this.self()), this.self());
    }

    private ActorRef createWordEndpointDistributor(final MasterCommand masterCommand) {
        int expectedNumberOfSlaves = masterCommand.getNumberOfSlaves();
        if (this.contributesWorkers) {
            expectedNumberOfSlaves++;
        }
        return this.context().actorOf(WordEndpointDistributor.props(expectedNumberOfSlaves));
    }

    private ActorRef createCorpusDistributor(final MasterCommand masterCommand) {
        int expectedNumberOfSlaves = masterCommand.getNumberOfSlaves();
        if (this.contributesWorkers) {
            expectedNumberOfSlaves++;
        }
        return this.context().actorOf(
                CorpusDistributor.props(expectedNumberOfSlaves, masterCommand.getPathToInputFile()));
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                .match(Shepherd.SlaveNodeRegistrationMessage.class, this::handle)
                .matchAny(this::redirectToWorkerCoordinator)
                .build();
    }

    private void handle(Shepherd.SlaveNodeRegistrationMessage message) {
        this.wordEndpointDistributor.tell(message, this.self());
        this.corpusDistributor.tell(message, this.self());
    }

    private void redirectToWorkerCoordinator(Object message) {
        this.workerCoordinator.tell(message, this.sender());
    }
}
