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
        this.log().info(String.format("Currently producing skip-grams for %d consumers",
                this.skipGramProducers.values().stream().filter(SkipGramProducer::hasNext).map(producer -> 1L).mapToLong(Long::longValue).sum()));

        SkipGramProducer responsibleProducer = this.skipGramProducers.get(skipGramReceiver.path().root());
        this.distributeSkipGrams(skipGramReceiver, responsibleProducer);

        if (this.skipGramProducers.values().stream().noneMatch(SkipGramProducer::hasNext)) {
            this.log().info("All skip-grams have been distributed.");
            this.context().parent().tell(new TrainingCoordinator.SkipGramsDistributed(), this.self());
        }
    }

    private void distributeSkipGrams(ActorRef skipGramReceiver, SkipGramProducer producer) {
        if (!producer.hasNext()) {
            skipGramReceiver.tell(new TrainingCoordinator.EndOfTraining(this.self()), this.self());
            this.log().info(String.format("No more skip-grams for %s", skipGramReceiver.path()));
            return;
        }

        List<UnencodedSkipGram> payload = producer.next();

        this.log().debug(String.format("Distributing %d skip-grams to %s", payload.size(), skipGramReceiver.path()));
        List<UnencodedSkipGram> unencodedSkipGrams = new ArrayList<>();
        ActorRef lastReceiver = skipGramReceiver;
        for (UnencodedSkipGram unencodedSkipGram : payload) {
            for (EncodedSkipGram encodedSkipGram : unencodedSkipGram.extractEncodedSkipGrams()) {
                skipGramReceiver.tell(SkipGramReceiver.ProcessEncodedSkipGram
                        .builder()
                        .skipGram(encodedSkipGram)
                        .wordEndpointResponsibleForInput(WordEndpointResolver.getInstance().localWordEndpoint())
                        .epoch(producer.getCurrentEpoch())
                        .build(), this.self());
            }

            unencodedSkipGrams.add(unencodedSkipGram);
        }

        this.log().debug(String.format("Grouping %d unencoded skip grams for %s by input resolver",
                unencodedSkipGrams.size(), skipGramReceiver.path()));

        for (Map.Entry<ActorRef, List<UnencodedSkipGram>> skipGramsByInputResolver :
                this.groupByInputResolver(unencodedSkipGrams).entrySet()) {

            lastReceiver = skipGramsByInputResolver.getKey();
            lastReceiver.tell(WordEndpoint.EncodeSkipGrams
                            .builder()
                            .unencodedSkipGrams(skipGramsByInputResolver.getValue())
                            .epoch(producer.getCurrentEpoch())
                            .build(), skipGramReceiver);
        }

        ActorRef responsibleEndpoint =
                WordEndpointResolver.getInstance().wordEndpointOf(skipGramReceiver.path().root());
        ActorRef lastEndpoint = WordEndpointResolver.getInstance().wordEndpointOf(lastReceiver.path().root());
        lastEndpoint.tell(new TrainingCoordinator.SkipGramChunkTransferred(this.self(), responsibleEndpoint),
                this.self());

        this.log().info(String.format("Informed %s about the end of this skip gram chunk produced for %s",
                lastEndpoint.path(), responsibleEndpoint.path()));
    }

    private Map<ActorRef, List<UnencodedSkipGram>> groupByInputResolver(List<UnencodedSkipGram> unencodedSkipGrams) {
        WordEndpointResolver wordEndpointResolver = WordEndpointResolver.getInstance();
        Map<ActorRef, Map<String, List<String>>> skipGramsToResolve = new HashMap<>();
        for (UnencodedSkipGram skipGram : unencodedSkipGrams) {
            for (String inputWord : skipGram.getInputs()) {
                ActorRef resolver = wordEndpointResolver.resolve(inputWord);
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
        this.log().info(String.format("Next skip gram batch requested from %s", this.sender().path()));
        this.createAndDistributeSkipGrams(this.sender());
    }
}
