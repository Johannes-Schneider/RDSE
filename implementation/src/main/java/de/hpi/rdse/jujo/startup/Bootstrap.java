package de.hpi.rdse.jujo.startup;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import com.typesafe.config.Config;
import de.hpi.rdse.jujo.actors.common.Reaper;
import de.hpi.rdse.jujo.actors.master.Master;
import de.hpi.rdse.jujo.actors.master.Shepherd;
import de.hpi.rdse.jujo.actors.slave.Sheep;
import de.hpi.rdse.jujo.actors.slave.Slave;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeoutException;

public class Bootstrap {

    private static final String DEFAULT_MASTER_SYSTEM_NAME = "MasterActorSystem";
    private static final String DEFAULT_SLAVE_SYSTEM_NAME = "SlaveActorSystem";

    public static void runMaster(MasterCommand masterCommand) {
        final Config config = ConfigurationFactory.createRemoteAkkaConfig(masterCommand.getHost(), MasterCommand.DEFAULT_PORT);
        final ActorSystem actorSystem = ActorSystem.create(DEFAULT_MASTER_SYSTEM_NAME, config);

        actorSystem.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);

        final ActorRef master = actorSystem.actorOf(
                Master.props(masterCommand),
                Master.DEFAULT_NAME
        );

        final ActorRef shepherd = actorSystem.actorOf(Shepherd.props(master), Shepherd.DEFAULT_NAME);

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
        final Config config = ConfigurationFactory.createRemoteAkkaConfig(slaveCommand.getDefaultHost(), SlaveCommand.DEFAULT_PORT);
        final ActorSystem actorSystem = ActorSystem.create(DEFAULT_SLAVE_SYSTEM_NAME, config);

        actorSystem.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);

        final ActorRef slave = actorSystem.actorOf(Slave.props(slaveCommand), Slave.DEFAULT_NAME);
        final ActorRef sheep = actorSystem.actorOf(Sheep.props(slave));

        sheep.tell(
                Sheep.RegisterAtShepherd.builder()
                        .shepherdAddress(new Address(
                                "akka.tcp",
                                DEFAULT_MASTER_SYSTEM_NAME,
                                slaveCommand.getMasterHost(),
                                MasterCommand.DEFAULT_PORT))
                        .build(),
                ActorRef.noSender()
        );

        Bootstrap.awaitTermination(actorSystem);
    }

}
