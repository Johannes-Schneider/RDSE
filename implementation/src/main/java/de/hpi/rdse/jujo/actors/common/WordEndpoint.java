package de.hpi.rdse.jujo.actors.common;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import de.hpi.rdse.jujo.wordManagement.WordEndpointResolver;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

public class WordEndpoint extends AbstractReapedActor {

    public static final String DEFAULT_NAME = "WordEndpoint";

    public static Props props() {
        return Props.create(WordEndpoint.class, WordEndpoint::new);
    }

    @Builder @NoArgsConstructor @AllArgsConstructor @Getter
    public static class WordEndpoints implements Serializable {
        private static final long serialVersionUID = 8070089151352318828L;
        private List<ActorRef> endpoints;
    }

    @NoArgsConstructor @AllArgsConstructor @Getter
    public static class VocabularyCreated implements Serializable {
        private static final long serialVersionUID = 5126582840330122184L;
        private Vocabulary vocabulary;
    }

    private final WordEndpointResolver wordEndpointResolver = new WordEndpointResolver(this.self());
    private final ActorRef subsampler;
    private final ActorRef vocabularyDistributor;
    private ActorRef vocabularyReceiver;
    private Vocabulary vocabulary;

    private WordEndpoint() {
        this.subsampler = this.context().actorOf(Subsampler.props(this.wordEndpointResolver));
        this.vocabularyDistributor = this.context().actorOf(VocabularyDistributor.props());
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
                   .matchAny(this::handleAny)
                   .build();
    }

    private void handle(WordEndpoints message) {
        if (this.wordEndpointResolver.isReadyToResolve()) {
            this.log().warning("Received WordEndpoints message although already received earlier.");
        }

        this.log().info("Received all WordEndpoints");
        this.wordEndpointResolver.setWordEndpoints(message.getEndpoints());
        this.subsampler.tell(message, this.sender());
    }

    private void handle(Subsampler.WordsCounted message) {
        this.sender().tell(PoisonPill.getInstance(), ActorRef.noSender());
        this.subsampler.tell(message, this.sender());
    }

    private void handle(VocabularyCreated message) {
        this.vocabulary = message.getVocabulary();
        this.sender().tell(PoisonPill.getInstance(), ActorRef.noSender());

        this.vocabularyDistributor.tell(
                VocabularyDistributor.DistributeVocabulary.builder()
                                                          .vocabulary(this.vocabulary)
                                                          .wordEndpointResolver(this.wordEndpointResolver)
                                                          .build(),
                this.self());
    }

    private void handle(Subsampler.TakeOwnershipForWordCounts message) {
        this.subsampler.tell(message, this.sender());
    }

    private void handle(Subsampler.ConfirmWordOwnershipDistribution message) {
        this.subsampler.tell(message, this.sender());
    }

    private void handle(VocabularyReceiver.ProcessVocabulary message) {
        if (this.vocabularyReceiver == null) {
            this.vocabularyReceiver = this.context().actorOf(VocabularyReceiver.props(this.vocabulary));
        }

        this.vocabularyReceiver.tell(message, this.sender());
    }
}
