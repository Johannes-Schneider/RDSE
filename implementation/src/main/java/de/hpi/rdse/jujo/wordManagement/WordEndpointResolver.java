package de.hpi.rdse.jujo.wordManagement;

import akka.actor.ActorRef;
import akka.actor.RootActorPath;
import com.google.common.hash.Hashing;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class WordEndpointResolver {

    private static final Logger Log = LogManager.getLogger(WordEndpointResolver.class);

    private static final ReentrantLock instanceLock = new ReentrantLock();
    private static WordEndpointResolver instance;

    public static void createInstance(ActorRef localWordEndpoint) {
        WordEndpointResolver.instanceLock.lock();
        try {
            if (WordEndpointResolver.instance != null) {
                Log.error("Tried to create a second instance of WordEndpointResolver!");
                return;
            }
            WordEndpointResolver.instance = new WordEndpointResolver(localWordEndpoint);
        }
        finally {
            WordEndpointResolver.instanceLock.unlock();
        }
    }

    public static WordEndpointResolver getInstance() {
        WordEndpointResolver.instanceLock.lock();
        try {
            if (WordEndpointResolver.instance == null) {
                Log.error("Called getInstance of WordEndpointResolver without calling createInstance first!");
            }
            return WordEndpointResolver.instance;
        }
        finally {
            WordEndpointResolver.instanceLock.unlock();
        }
    }

    private final List<ActorRef> wordEndpoints = new ArrayList<>();
    private final ActorRef localWordEndpoint;
    @Getter @Setter
    private ActorRef master = ActorRef.noSender();

    private WordEndpointResolver(ActorRef localWordEndpoint) {
        this.localWordEndpoint = localWordEndpoint;
    }

    public ActorRef resolve(String word) {
        if (!this.isReadyToResolve()) {
            return ActorRef.noSender();
        }
        return this.wordEndpoints.get(this.wordEndpointIndex(word));
    }

    private int wordEndpointIndex(String word) {
        return Hashing.consistentHash(Vocabulary.hash(word), wordEndpoints.size());
    }

    public boolean isReadyToResolve() {
        return !this.wordEndpoints.isEmpty();
    }

    public List<ActorRef> all() {
        return this.wordEndpoints;
    }

    public ActorRef localWordEndpoint() {
        return this.localWordEndpoint;
    }

    public ActorRef wordEndpointOf(RootActorPath remote) {
        return this.all().stream().filter(e -> e.path().root().equals(remote)).findFirst().orElseThrow(() -> new RuntimeException(String.format("Unknown remote actor path: %s", remote)));
    }

}
