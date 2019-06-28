package de.hpi.rdse.jujo.actors.common.training;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.training.EncodedSkipGram;
import de.hpi.rdse.jujo.training.SkipGramProducer;
import de.hpi.rdse.jujo.training.UnencodedSkipGram;
import de.hpi.rdse.jujo.wordManagement.WordEndpointResolver;
import lombok.NoArgsConstructor;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SkipGramDistributor extends AbstractReapedActor {

    public static Props props(String localCorpusPartitionPath) {
        return Props.create(SkipGramDistributor.class, () -> new SkipGramDistributor(localCorpusPartitionPath));
    }

    @NoArgsConstructor
    public static class RequestNextSkipGramChunk implements Serializable {
        private static final long serialVersionUID = -4382367275556082887L;
    }

    private final Map<ActorRef, SkipGramProducer> skipGramProducers = new HashMap<>();

    private SkipGramDistributor(String localCorpusPartitionPath) throws FileNotFoundException {
        for (ActorRef skipGramReceiver : WordEndpointResolver.getInstance().all()) {
            this.skipGramProducers.put(skipGramReceiver, new SkipGramProducer(skipGramReceiver,
                    localCorpusPartitionPath));
        }
        this.startSkipGramDistribution();
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(RequestNextSkipGramChunk.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private void startSkipGramDistribution() {
        for (ActorRef skipGramReceiver : this.skipGramProducers.keySet()) {
            this.createAndDistributeSkipGrams(skipGramReceiver);
        }
    }

    private void createAndDistributeSkipGrams(ActorRef skipGramReceiver) {
        if (this.skipGramProducers.values().stream().noneMatch(SkipGramProducer::hasNext)) {
            this.context().parent().tell(new TrainingCoordinator.SkipGramsDistributed(), this.self());
            return;
        }
        this.distributeSkipGrams(skipGramReceiver, this.skipGramProducers.get(skipGramReceiver).next());
    }

    private void distributeSkipGrams(ActorRef skipGramReceiver, List<UnencodedSkipGram> payload) {
        this.log().debug(String.format("Distributing %d skip-grams to %s", payload.size(), skipGramReceiver.path()));
        SkipGramReceiver.ProcessUnencodedSkipGrams message = new SkipGramReceiver.ProcessUnencodedSkipGrams();
        for (UnencodedSkipGram unencodedSkipGram : payload) {
            for (EncodedSkipGram encodedSkipGram : unencodedSkipGram.extractEncodedSkipGrams()) {
                skipGramReceiver.tell(new SkipGramReceiver.ProcessEncodedSkipGram(encodedSkipGram),
                        WordEndpointResolver.getInstance().localWordEndpoint());
            }
            message.getSkipGrams().add(unencodedSkipGram);
        }
        skipGramReceiver.tell(message, this.self());
        skipGramReceiver.tell(new SkipGramReceiver.SkipGramChunkTransferred(), this.self());
    }

    private void handle(RequestNextSkipGramChunk message) {
        this.createAndDistributeSkipGrams(this.sender());
    }
}
