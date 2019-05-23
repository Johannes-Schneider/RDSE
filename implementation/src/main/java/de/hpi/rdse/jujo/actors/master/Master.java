package de.hpi.rdse.jujo.actors.master;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rdse.jujo.actors.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.WordEndpoint;
import de.hpi.rdse.jujo.actors.slave.CorpusReceiver;
import de.hpi.rdse.jujo.utils.startup.MasterCommand;
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
    private final ActorRef wordEndpoint;
    private final ActorRef corpusDistributor;
    private final ActorRef corpusReceiver;
    private final boolean contributesWorkers;

    private Master(MasterCommand masterCommand) {
        this.contributesWorkers = masterCommand.getNumberOfWorkers() > 0;
        this.wordEndpoint = this.context().actorOf(WordEndpoint.props(), WordEndpoint.DEFAULT_NAME);
        this.wordEndpointDistributor = this.createWordEndpointDistributor(masterCommand);
        this.corpusDistributor = this.createCorpusDistributor(masterCommand);
        this.corpusReceiver = this.context().actorOf(
                CorpusReceiver.props(masterCommand.getTemporaryWorkingDirectory()));
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
                .match(CorpusReceiver.ProcessCorpusPartition.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(CorpusReceiver.ProcessCorpusPartition message) {
        this.corpusReceiver.tell(message, this.sender());
    }

    private void handle(Shepherd.SlaveNodeRegistrationMessage message) {
        this.wordEndpointDistributor.tell(message, this.self());
        this.corpusDistributor.tell(message, this.self());
    }
}
