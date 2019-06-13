package de.hpi.rdse.jujo.actors.common.training;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.fileHandling.FileWordIterator;
import de.hpi.rdse.jujo.training.UnencodedSkipGram;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SkipGramDistributor extends AbstractReapedActor {

    private static final int WINDOW_SIZE = 5; // TODO: change this to be a CLI argument

    public static Props props(ActorRef supervisor, String localCorpusPartitionPath, Vocabulary vocabulary) {
        return Props.create(SkipGramDistributor.class, () -> new SkipGramDistributor(supervisor,
                                                                                     localCorpusPartitionPath,
                                                                                     vocabulary));
    }

    private final ActorRef supervisor;
    private final Vocabulary vocabulary;
    private final FileWordIterator fileIterator;
    private final List<String> words = new ArrayList<>();
    private final Map<ActorRef, List<UnencodedSkipGram>> skipGramsByResponsibleWordEndpoint = new HashMap<>();

    private SkipGramDistributor(ActorRef supervisor,
                                String localCorpusPartitionPath,
                                Vocabulary vocabulary) throws FileNotFoundException {
        this.supervisor = supervisor;
        this.vocabulary = vocabulary;
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
            this.supervisor.tell(new TrainingCoordinator.SkipGramsDistributed(), this.self());
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

        this.words.subList(0, this.words.size() - WINDOW_SIZE).clear();
    }

    private UnencodedSkipGram createSkipGramForWordAt(int wordIndex) {
        String expectedOutput = this.words.get(wordIndex);
        if (!this.vocabulary.contains(expectedOutput)) {
            return UnencodedSkipGram.Empty();
        }

        UnencodedSkipGram skipGram = new UnencodedSkipGram(expectedOutput);
        int startIndex = Math.max(0, wordIndex - WINDOW_SIZE);
        int endIndex = Math.min(this.words.size() - 1, wordIndex + WINDOW_SIZE);

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
