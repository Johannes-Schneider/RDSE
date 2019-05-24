package de.hpi.rdse.jujo.actors.slave;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.remote.DisassociatedEvent;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.common.WorkerCoordinator;
import de.hpi.rdse.jujo.startup.SlaveCommand;

public class Slave extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "slave";

    public static Props props(SlaveCommand slaveCommand) {
        return Props.create(Slave.class, () -> new Slave(slaveCommand));
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        this.getContext().getSystem().eventStream().subscribe(this.getSelf(), DisassociatedEvent.class);
    }

    private final ActorRef workerCoordinator;

    private Slave(SlaveCommand slaveCommand) {
        this.workerCoordinator = this.context().actorOf(
                WorkerCoordinator.props(slaveCommand.getTemporaryWorkingDirectory()));
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                .match(DisassociatedEvent.class, this::handle)
                .matchAny(this::redirectToWorkerCoordinator)
                .build();
    }

    private void handle(DisassociatedEvent event) {
        this.log().error("Disassociated from master. Stopping Slave ...");
        this.getContext().stop(self());
    }

    private void redirectToWorkerCoordinator(Object message) {
        this.workerCoordinator.tell(message, this.sender());
    }
}
