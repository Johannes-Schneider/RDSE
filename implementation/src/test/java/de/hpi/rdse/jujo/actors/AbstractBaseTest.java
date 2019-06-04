package de.hpi.rdse.jujo.actors;

import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.Config;
import de.hpi.rdse.jujo.actors.common.Reaper;
import de.hpi.rdse.jujo.startup.ConfigurationFactory;
import org.junit.After;
import org.junit.Before;

public abstract class AbstractBaseTest {

    protected static ActorSystem actorSystem;

    @Before
    public void setUp() {
        Config config = ConfigurationFactory.createRemoteAkkaConfig("localhost", 7787);
        actorSystem = ActorSystem.create("TestSystem", config);

        actorSystem.actorOf(Reaper.props(), Reaper.DEFAULT_NAME);
    }

    @After
    public void tearDown() {
        TestKit.shutdownActorSystem(actorSystem);
        actorSystem = null;
    }
}
