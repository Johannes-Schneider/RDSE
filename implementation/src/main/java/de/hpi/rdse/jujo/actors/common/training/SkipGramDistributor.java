package de.hpi.rdse.jujo.actors.common.training;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.RootActorPath;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.common.WordEndpoint;
import de.hpi.rdse.jujo.training.EncodedSkipGram;
import de.hpi.rdse.jujo.training.SkipGramProducer;
import de.hpi.rdse.jujo.training.UnencodedSkipGram;
import de.hpi.rdse.jujo.wordManagement.WordEndpointResolver;
import lombok.NoArgsConstructor;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
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

    private final Map<RootActorPath, SkipGramProducer> skipGramProducers = new HashMap<>();

    private SkipGramDistributor(String localCorpusPartitionPath) throws FileNotFoundException {
        for (ActorRef skipGramReceiver : WordEndpointResolver.getInstance().all()) {
            RootActorPath remote = skipGramReceiver.path().root();
            this.skipGramProducers.put(remote, new SkipGramProducer(remote, localCorpusPartitionPath));
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
        for (RootActorPath remote : this.skipGramProducers.keySet()) {
            this.createAndDistributeSkipGrams(WordEndpointResolver.getInstance().wordEndpointOf(remote));
        }
    }

    private void createAndDistributeSkipGrams(ActorRef skipGramReceiver) {
        if (this.skipGramProducers.values().stream().noneMatch(SkipGramProducer::hasNext)) {
            this.log().info("All skip-grams have been distributed.");
            this.context().parent().tell(new TrainingCoordinator.SkipGramsDistributed(), this.self());
            return;
        }
        this.distributeSkipGrams(skipGramReceiver, this.skipGramProducers.get(skipGramReceiver.path().root()).next());
    }

    private void distributeSkipGrams(ActorRef skipGramReceiver, List<UnencodedSkipGram> payload) {
        this.log().debug(String.format("Distributing %d skip-grams to %s", payload.size(), skipGramReceiver.path()));
        ActorRef lastReceiver = skipGramReceiver;
        for (UnencodedSkipGram unencodedSkipGram : payload) {
            for (EncodedSkipGram encodedSkipGram : unencodedSkipGram.extractEncodedSkipGrams()) {
                skipGramReceiver.tell(new SkipGramReceiver.ProcessEncodedSkipGram(encodedSkipGram),
                                      WordEndpointResolver.getInstance().localWordEndpoint());
            }
        }

        for (Map.Entry<ActorRef, List<UnencodedSkipGram>> skipGramsByInputResolver :
                this.groupByInputResolver(payload).entrySet()) {

            lastReceiver = skipGramsByInputResolver.getKey();
            lastReceiver.tell(new WordEndpoint.EncodeSkipGrams(skipGramsByInputResolver.getValue()), skipGramReceiver);
        }

        ActorRef responsibleEndpoint =
                WordEndpointResolver.getInstance().wordEndpointOf(skipGramReceiver.path().root());
        ActorRef lastEndpoint = WordEndpointResolver.getInstance().wordEndpointOf(lastReceiver.path().root());
        lastEndpoint.tell(new TrainingCoordinator.SkipGramChunkTransferred(), responsibleEndpoint);
    }

    private Map<ActorRef, List<UnencodedSkipGram>> groupByInputResolver(List<UnencodedSkipGram> unencodedSkipGrams) {
        Map<ActorRef, Map<String, List<String>>> skipGramsToResolve = new HashMap<>();
        for (UnencodedSkipGram skipGram : unencodedSkipGrams) {
            for (String inputWord : skipGram.getInputs()) {
                ActorRef resolver = WordEndpointResolver.getInstance().resolve(inputWord);
                skipGramsToResolve.putIfAbsent(resolver, new HashMap<>());
                skipGramsToResolve.get(resolver).putIfAbsent(skipGram.getExpectedOutput(), new ArrayList<>());
                skipGramsToResolve.get(resolver).get(skipGram.getExpectedOutput()).add(inputWord);
            }
        }

        Map<ActorRef, List<UnencodedSkipGram>> skipGramsByInputResolver = new HashMap<>();
        for (Map.Entry<ActorRef, Map<String, List<String>>> entry : skipGramsToResolve.entrySet()) {
            ActorRef resolver = entry.getKey();

            for (Map.Entry<String, List<String>> rawSkipGram : entry.getValue().entrySet()) {
                UnencodedSkipGram skipGram = new UnencodedSkipGram(rawSkipGram.getKey());
                skipGram.getInputs().addAll(rawSkipGram.getValue());

                skipGramsByInputResolver.putIfAbsent(resolver, new ArrayList<>());
                skipGramsByInputResolver.get(resolver).add(skipGram);
            }
        }

        return skipGramsByInputResolver;
    }

    private void handle(RequestNextSkipGramChunk message) {
        this.createAndDistributeSkipGrams(this.sender());
    }
}
