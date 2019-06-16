package de.hpi.rdse.jujo.actors.common.training;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.actors.common.WordEndpoint;
import de.hpi.rdse.jujo.training.EncodedSkipGram;
import de.hpi.rdse.jujo.training.UnencodedSkipGram;
import de.hpi.rdse.jujo.training.Word2VecModel;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import de.hpi.rdse.jujo.wordManagement.WordEndpointResolver;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SkipGramReceiver extends AbstractReapedActor {

    public static Props props() {
        return Props.create(SkipGramReceiver.class, SkipGramReceiver::new);
    }

    @NoArgsConstructor @AllArgsConstructor @Getter
    public static class ProcessUnencodedSkipGrams implements Serializable {
        private static final long serialVersionUID = 735332284132943544L;
        private List<UnencodedSkipGram> skipGrams = new ArrayList<>();
    }

    @NoArgsConstructor @AllArgsConstructor @Getter
    public static class ProcessEncodedSkipGram implements Serializable {
        private static final long serialVersionUID = -6574596641614399323L;
        private EncodedSkipGram skipGram;
    }

    @NoArgsConstructor
    public static class SkipGramChunkTransferred implements Serializable {
        private static final long serialVersionUID = -3803848151388038254L;
    }

    @NoArgsConstructor
    public static class RequestNextSkipGramChunk implements Serializable {
        private static final long serialVersionUID = -4382367275556082887L;
    }

    private final Map<ActorRef, Map<String, List<String>>> unencodedSkipGramsByActor = new HashMap<>();

    private SkipGramReceiver() {}

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(ProcessEncodedSkipGram.class, this::handle)
                   .match(ProcessUnencodedSkipGrams.class, this::handle)
                   .match(SkipGramChunkTransferred.class, this::handle)
                   .match(RequestNextSkipGramChunk.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private void handle(ProcessEncodedSkipGram message) {
        Word2VecModel.getInstance().train(message.getSkipGram());
    }

    private void handle(ProcessUnencodedSkipGrams message) {
        if (!Vocabulary.getInstance().isComplete()) {
            this.self().tell(message, this.sender());
            return;
        }
        for (UnencodedSkipGram unencodedSkipGram : message.getSkipGrams()) {
            for (EncodedSkipGram encodedSkipGram : unencodedSkipGram.extractEncodedSkipGrams()) {
                this.self().tell(new ProcessEncodedSkipGram(encodedSkipGram), this.self());
            }
            this.addToUnencodedSkipGramsToResolve(unencodedSkipGram);
        }
        this.resolveUnencodedSkipGrams();
    }

    private void addToUnencodedSkipGramsToResolve(UnencodedSkipGram skipGram) {
        for (String inputWord : skipGram.getInputs()) {
            ActorRef receiver = WordEndpointResolver.getInstance().resolve(inputWord);
            this.unencodedSkipGramsByActor.putIfAbsent(receiver, new HashMap<>());
            this.unencodedSkipGramsByActor.get(receiver).putIfAbsent(skipGram.getExpectedOutput(), new ArrayList<>());
            this.unencodedSkipGramsByActor.get(receiver).get(skipGram.getExpectedOutput()).add(inputWord);
        }
    }

    private void resolveUnencodedSkipGrams() {
        for (ActorRef resolver : this.unencodedSkipGramsByActor.keySet()) {
            WordEndpoint.EncodeSkipGrams message = new WordEndpoint.EncodeSkipGrams();
            for (String expectedOutput : this.unencodedSkipGramsByActor.get(resolver).keySet()) {
                UnencodedSkipGram unencodedSkipGram = new UnencodedSkipGram(expectedOutput);
                unencodedSkipGram.getInputs().addAll(this.unencodedSkipGramsByActor.get(resolver).get(expectedOutput));
                message.getUnencodedSkipGrams().add(unencodedSkipGram);
            }
            resolver.tell(message, this.self());
        }
        this.unencodedSkipGramsByActor.clear();
    }

    private void handle(SkipGramChunkTransferred message) {
        this.self().tell(new RequestNextSkipGramChunk(), this.sender());
    }

    private void handle(RequestNextSkipGramChunk message) {
        this.sender().tell(message, this.self());
    }

}
