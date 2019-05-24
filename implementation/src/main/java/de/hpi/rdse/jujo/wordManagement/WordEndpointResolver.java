package de.hpi.rdse.jujo.wordManagement;

import akka.actor.ActorRef;
import com.google.common.hash.Hashing;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Setter
public class WordEndpointResolver {

    private List<ActorRef> wordEndpoints = new ArrayList<>();
    private final ActorRef localWordEndpoint;

    public WordEndpointResolver(ActorRef localWordEndpoint) {
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
