package de.hpi.rdse.jujo;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import com.typesafe.config.Config;
import de.hpi.rdse.jujo.actors.Master;
import de.hpi.rdse.jujo.actors.Reaper;
import de.hpi.rdse.jujo.actors.Shepherd;
import de.hpi.rdse.jujo.actors.Slave;
import de.hpi.rdse.jujo.utils.AkkaUtils;
import de.hpi.rdse.jujo.utils.startup.MasterCommand;
import de.hpi.rdse.jujo.utils.startup.SlaveCommand;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeoutException;

public class Bootstrap {

    private static final String DEFAULT_MASTER_SYSTEM_NAME = "MasterActorSystem";
    private static final String DEFAULT_SLAVE_SYSTEM_NAME = "SlaveActorSystem";

    public static void runMaster(MasterCommand masterCommand) {
        // Create the ActorSystem
        final Config config = AkkaUtils.createRemoteAkkaConfig(masterCommand.getHost(), MasterCommand.DEFAULT_PORT);
        final ActorSystem actorSystem = ActorSystem.create(DEFAULT_MASTER_SYSTEM_NAME, config);

        // Create the Reaper.
        actorSystem.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);

        // Create the Master
        final ActorRef master = actorSystem.actorOf(
                Master.props(masterCommand),
                Master.DEFAULT_NAME
        );

        // Create the Shepherd
        final ActorRef shepherd = actorSystem.actorOf(Shepherd.props(master), Shepherd.DEFAULT_NAME);

        // Await termination: The termination should be issued by the reaper
        Bootstrap.awaitTermination(actorSystem);
    }


    public static void awaitTermination(final ActorSystem actorSystem) {
        try {
            Await.ready(actorSystem.whenTerminated(), Duration.Inf());
        } catch (TimeoutException | InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        System.out.println("ActorSystem terminated!");
    }

    public static void runSlave(SlaveCommand slaveCommand) {
        // Create the local ActorSystem
        final Config config = AkkaUtils.createRemoteAkkaConfig(slaveCommand.getDefaultHost(), SlaveCommand.DEFAULT_PORT);
        final ActorSystem actorSystem = ActorSystem.create(DEFAULT_SLAVE_SYSTEM_NAME, config);

        // Create the reaper.
        actorSystem.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);

        // Create a Slave
        final ActorRef slave = actorSystem.actorOf(Slave.props(), Slave.DEFAULT_NAME);

        // Tell the Slave endPassword register the local ActorSystem
        slave.tell(
                Slave.RegisterAtShepherdMessage.builder()
                        .numberOfLocalWorkers(slaveCommand.getNumberOfWorkers())
                        .shepherdAddress(new Address(
                                "akka.tcp",
                                DEFAULT_MASTER_SYSTEM_NAME,
                                slaveCommand.getMasterHost(),
                                MasterCommand.DEFAULT_PORT))
                        .build(),
                ActorRef.noSender()
        );

        // Await termination: The termination should be issued by the reaper
        Bootstrap.awaitTermination(actorSystem);
    }

}
