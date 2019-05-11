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
        final Config config = AkkaUtils.createRemoteAkkaConfig(masterCommand.getHost(), MasterCommand.DEFAULT_PORT);
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
        final Config config = AkkaUtils.createRemoteAkkaConfig(slaveCommand.getDefaultHost(), SlaveCommand.DEFAULT_PORT);
        final ActorSystem actorSystem = ActorSystem.create(DEFAULT_SLAVE_SYSTEM_NAME, config);

        actorSystem.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);

        final ActorRef slave = actorSystem.actorOf(Slave.props(slaveCommand), Slave.DEFAULT_NAME);

        slave.tell(
                Slave.RegisterAtShepherd.builder()
                        .numberOfLocalWorkers(slaveCommand.getNumberOfWorkers())
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
