package de.hpi.rdse.jujo.actors.common;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.RootActorPath;
import de.hpi.rdse.jujo.actors.common.training.SkipGramReceiver;
import de.hpi.rdse.jujo.actors.common.training.TrainingCoordinator;
import de.hpi.rdse.jujo.training.EncodedSkipGram;
import de.hpi.rdse.jujo.training.UnencodedSkipGram;
import de.hpi.rdse.jujo.training.Word2VecModel;
import de.hpi.rdse.jujo.training.WordEmbedding;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import de.hpi.rdse.jujo.wordManagement.WordEndpointResolver;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.math3.linear.RealVector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class WordEndpoint extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "WordEndpoint";

    public static Props props() {
        return Props.create(WordEndpoint.class, WordEndpoint::new);
    }

    @Builder @NoArgsConstructor @AllArgsConstructor @Getter
    public static class WordEndpoints implements Serializable {
        private static final long serialVersionUID = 8070089151352318828L;
        private List<ActorRef> endpoints;
        private ActorRef master;
    }

    @NoArgsConstructor
    public static class VocabularyCreated implements Serializable {
        private static final long serialVersionUID = 5126582840330122184L;
    }

    @NoArgsConstructor
    public static class VocabularyCompleted implements Serializable {
        private static final long serialVersionUID = -8133305903846340892L;
    }

    @NoArgsConstructor @AllArgsConstructor @Builder @Getter
    public static class EncodeSkipGrams implements Serializable {
        private static final long serialVersionUID = 4648091561498065299L;
        @Builder.Default
        private List<UnencodedSkipGram> unencodedSkipGrams = new ArrayList<>();
        private int epoch;
    }

    @NoArgsConstructor @AllArgsConstructor @Builder @Getter
    public static class UpdateWeight implements Serializable {
        private static final long serialVersionUID = -6193882947371330180L;
        private long oneHotIndex;
        private RealVector gradient;
        private int epoch;
    }

    @NoArgsConstructor
    public static class Unsubscribe implements Serializable {
        private static final long serialVersionUID = -3790931380719047330L;
    }

    private final ActorRef subsampler;
    private final ActorRef vocabularyDistributor;
    private ActorRef vocabularyReceiver;
    private final Set<RootActorPath> subscribers = new HashSet<>();

    private WordEndpoint() {
        WordEndpointResolver.createInstance(this.self());
        this.subsampler = this.spawnChild(Subsampler.props());
        this.vocabularyDistributor = this.spawnChild(VocabularyDistributor.props());
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(WordEndpoints.class, this::handle)
                   .match(Subsampler.WordsCounted.class, this::handle)
                   .match(VocabularyCreated.class, this::handle)
                   .match(Subsampler.TakeOwnershipForWordCounts.class, this::handle)
                   .match(Subsampler.ConfirmWordOwnershipDistribution.class, this::handle)
                   .match(VocabularyReceiver.ProcessVocabulary.class, this::handle)
                   .match(VocabularyCompleted.class, this::handle)
                   .match(SkipGramReceiver.ProcessEncodedSkipGram.class, this::handle)
                   .match(TrainingCoordinator.SkipGramChunkTransferred.class, this::handle)
                   .match(EncodeSkipGrams.class, this::handle)
                   .match(UpdateWeight.class, this::handle)
                   .match(TrainingCoordinator.EndOfTraining.class, this::handle)
                   .match(Unsubscribe.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private void handle(WordEndpoints message) {
        if (WordEndpointResolver.getInstance().isReadyToResolve()) {
            this.log().warning("Received WordEndpoints message although already received earlier.");
        }

        this.log().info("Received all WordEndpoints");
        WordEndpointResolver.getInstance().all().addAll(message.getEndpoints());
        WordEndpointResolver.getInstance().setMaster(message.getMaster());
        this.subsampler.tell(message, this.sender());
        this.subscribers.addAll(WordEndpointResolver.getInstance().all().stream()
                                                    .map(ref -> ref.path().root())
                                                    .collect(Collectors.toCollection(LinkedList::new)));
    }

    private void handle(Subsampler.WordsCounted message) {
        this.subsampler.tell(message, this.sender());
    }

    private void handle(VocabularyCreated message) {
        this.vocabularyDistributor.tell(new VocabularyDistributor.DistributeVocabulary(), this.self());
        this.context().parent().tell(message, this.self());
        if (Vocabulary.getInstance().isComplete()) {
            this.log().info("Vocabulary completed. Informing WordEndpoint.");
            this.self().tell(new WordEndpoint.VocabularyCompleted(), this.self());
        }
    }

    private void handle(VocabularyCompleted message) {
        this.context().parent().tell(new WorkerCoordinator.VocabularyReadyForTraining(), this.self());
    }

    private void handle(VocabularyReceiver.ProcessVocabulary message) {
        if (this.vocabularyReceiver == null) {
            this.vocabularyReceiver = this.spawnChild(VocabularyReceiver.props());
        }

        this.vocabularyReceiver.tell(message, this.sender());
    }

    private void handle(EncodeSkipGrams message) {
        for (UnencodedSkipGram unencodedSkipGram : message.getUnencodedSkipGrams()) {
            for (String input : unencodedSkipGram.getInputs()) {
                if (!Vocabulary.getInstance().containsLocally(input)) {
                    continue;
                }
                WordEmbedding embeddedInput = Word2VecModel.getInstance().createInputEmbedding(input);
                EncodedSkipGram encodedSkipGram = new EncodedSkipGram(unencodedSkipGram.getExpectedOutput(), embeddedInput);
                this.sender().tell(SkipGramReceiver.ProcessEncodedSkipGram
                        .builder()
                        .skipGram(encodedSkipGram)
                        .wordEndpointResponsibleForInput(this.self())
                        .epoch(message.getEpoch())
                        .build(), this.self());
            }
        }
        this.log().debug(String.format("Successfully encoded %d skip-grams", message.getUnencodedSkipGrams().size()));
    }

    private void handle(UpdateWeight message) {
        int localOneHotIndex = Vocabulary.getInstance().toLocalOneHotIndex(message.getOneHotIndex());
        Word2VecModel.getInstance().updateInputWeight(localOneHotIndex, message.getGradient(), message.getEpoch());
    }

    private void handle(TrainingCoordinator.SkipGramChunkTransferred message) {
        this.log().debug(String.format("Skip gram chunk transferred from %s to %s", message.getProducer(),
                message.getConsumer()));
        if (this.self().path().root().equals(message.getConsumer().path().root())) {
            this.context().parent().tell(message, this.sender());
            return;
        }

        message.getConsumer().tell(message, this.self());
    }

    private void handle(Unsubscribe message) {
        this.subscribers.remove(this.sender().path().root());
        this.log().info(String.format("%s unsubscribed from local wordEndpoint. %d remaining subscribers",
                this.sender().path().root(), this.subscribers.size()));
        if (this.subscribers.isEmpty()) {
            this.purposeHasBeenFulfilled();
        }
    }

    // Forwarding
    private void handle(Subsampler.TakeOwnershipForWordCounts message) {
        this.subsampler.tell(message, this.sender());
    }

    private void handle(Subsampler.ConfirmWordOwnershipDistribution message) {
        this.subsampler.tell(message, this.sender());
    }

    private void handle(SkipGramReceiver.ProcessEncodedSkipGram message) {
        this.context().parent().tell(message, this.sender());
    }

    private void handle(TrainingCoordinator.EndOfTraining message) {
        this.context().parent().tell(message, this.sender());
    }
}
