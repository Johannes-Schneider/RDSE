package de.hpi.rdse.jujo.actors.common.training;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.fileHandling.FileWordIterator;
import de.hpi.rdse.jujo.training.UnencodedSkipGram;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SkipGramDistributor extends AbstractReapedActor {

    private static final int MAX_NUMBER_OF_SKIP_GRAMS_PER_MESSAGE = 100;

    public static Props props(String localCorpusPartitionPath, Vocabulary vocabulary, int windowSize) {
        return Props.create(SkipGramDistributor.class, () -> new SkipGramDistributor(localCorpusPartitionPath,
                vocabulary, windowSize));
    }

    private final Vocabulary vocabulary;
    private final FileWordIterator fileIterator;
    private final List<String> words = new ArrayList<>();
    private final Map<ActorRef, List<UnencodedSkipGram>> skipGramsByResponsibleWordEndpoint = new HashMap<>();
    private final int windowSize;

    private SkipGramDistributor(String localCorpusPartitionPath,
                                Vocabulary vocabulary, int windowSize) throws FileNotFoundException {
        this.vocabulary = vocabulary;
        this.windowSize = windowSize;
        this.fileIterator = new FileWordIterator(localCorpusPartitionPath);
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .matchAny(this::handleAny)
                   .build();
    }

    private void createSkipGrams() {
        if (!this.fileIterator.hasNext()) {
            this.context().parent().tell(new TrainingCoordinator.SkipGramsDistributed(), this.self());
            return;
        }

        this.words.addAll(Arrays.asList(this.fileIterator.next()));

        for (int i = 0; i < this.words.size(); ++i) {
            UnencodedSkipGram skipGram = this.createSkipGramForWordAt(i);
            if (skipGram.isEmpty()) {
                continue;
            }

            ActorRef responsibleWordEndpoint =
                    vocabulary.getWordEndpointResolver().resolve(skipGram.getExpectedOutput());
            this.skipGramsByResponsibleWordEndpoint.putIfAbsent(responsibleWordEndpoint, new ArrayList<>());
            this.skipGramsByResponsibleWordEndpoint.get(responsibleWordEndpoint).add(skipGram);
        }

        this.words.subList(0, this.words.size() - this.windowSize).clear();
    }

    private UnencodedSkipGram createSkipGramForWordAt(int wordIndex) {
        String expectedOutput = this.words.get(wordIndex);
        if (!this.vocabulary.contains(expectedOutput)) {
            return UnencodedSkipGram.Empty();
        }

        UnencodedSkipGram skipGram = new UnencodedSkipGram(expectedOutput);
        int startIndex = Math.max(0, wordIndex - this.windowSize);
        int endIndex = Math.min(this.words.size() - 1, wordIndex + this.windowSize);

        for (int i = startIndex; i <= endIndex; ++i) {
            if (i == wordIndex) {
                continue;
            }

            String input = this.words.get(i);
            if (!this.vocabulary.contains(input)) {
                continue;
            }

            skipGram.getInputs().add(input);
        }

        return skipGram;
    }
}
