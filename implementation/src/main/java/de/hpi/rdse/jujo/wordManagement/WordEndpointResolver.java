package de.hpi.rdse.jujo.wordManagement;

import akka.actor.ActorRef;
import com.google.common.hash.Hashing;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class WordEndpointResolver {

    private static final Logger Log = LogManager.getLogger(WordEndpointResolver.class);

    private static WordEndpointResolver instance;

    public static void createInstance(ActorRef localWordEndpoint) {
        if (WordEndpointResolver.instance != null) {
            Log.error("Tried to create a second instance of WordEndpointResolver!");
            return;
        }
        WordEndpointResolver.instance = new WordEndpointResolver(localWordEndpoint);
    }

    public static WordEndpointResolver getInstance() {
        if (WordEndpointResolver.instance == null) {
            Log.error("Called getInstance of WordEndpointResolver without calling createInstance first!");
        }
        return WordEndpointResolver.instance;
    }

    private List<ActorRef> wordEndpoints = new ArrayList<>();
    private final ActorRef localWordEndpoint;

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

}
