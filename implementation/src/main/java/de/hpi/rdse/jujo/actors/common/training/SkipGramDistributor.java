package de.hpi.rdse.jujo.actors.common.training;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.fileHandling.FileWordIterator;
import de.hpi.rdse.jujo.training.EncodedSkipGram;
import de.hpi.rdse.jujo.training.UnencodedSkipGram;
import de.hpi.rdse.jujo.training.Word2VecModel;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import de.hpi.rdse.jujo.wordManagement.WordEndpointResolver;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SkipGramDistributor extends AbstractReapedActor {

    public static Props props(String localCorpusPartitionPath) {
        return Props.create(SkipGramDistributor.class, () -> new SkipGramDistributor(localCorpusPartitionPath));
    }

    private final FileWordIterator fileIterator;
    private final List<String> words = new ArrayList<>();
    private final Map<ActorRef, List<UnencodedSkipGram>> skipGramsByResponsibleWordEndpoint = new HashMap<>();

    private SkipGramDistributor(String localCorpusPartitionPath) throws FileNotFoundException {
        this.fileIterator = new FileWordIterator(localCorpusPartitionPath);
        this.startSkipGramDistribution();
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                   .match(SkipGramReceiver.RequestNextSkipGramChunk.class, this::handle)
                   .matchAny(this::handleAny)
                   .build();
    }

    private void startSkipGramDistribution() {
        this.createSkipGrams();
        for (Map.Entry<ActorRef, List<UnencodedSkipGram>> entry : this.skipGramsByResponsibleWordEndpoint.entrySet()) {
            this.distributeSkipGrams(entry.getKey(), entry.getValue());
        }
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
                    WordEndpointResolver.getInstance().resolve(skipGram.getExpectedOutput());
            this.skipGramsByResponsibleWordEndpoint.putIfAbsent(responsibleWordEndpoint, new ArrayList<>());
            this.skipGramsByResponsibleWordEndpoint.get(responsibleWordEndpoint).add(skipGram);
        }

        this.words.subList(0, this.words.size() - Word2VecModel.getInstance().getConfiguration().getWindowSize()).clear();
    }

    private UnencodedSkipGram createSkipGramForWordAt(int wordIndex) {
        String expectedOutput = this.words.get(wordIndex);
        if (!Vocabulary.getInstance().contains(expectedOutput)) {
            return UnencodedSkipGram.empty();
        }

        UnencodedSkipGram skipGram = new UnencodedSkipGram(expectedOutput);
        int startIndex = Math.max(0, wordIndex - Word2VecModel.getInstance().getConfiguration().getWindowSize());
        int endIndex = Math.min(this.words.size() - 1, wordIndex + Word2VecModel.getInstance().getConfiguration().getWindowSize());

        for (int i = startIndex; i <= endIndex; ++i) {
            if (i == wordIndex) {
                continue;
            }

            String input = this.words.get(i);
            if (!Vocabulary.getInstance().contains(input)) {
                continue;
            }

            skipGram.getInputs().add(input);
        }

        return skipGram;
    }

    private void distributeSkipGrams(ActorRef responsibleEndpoint, List<UnencodedSkipGram> payload) {
        SkipGramReceiver.ProcessUnencodedSkipGrams message = new SkipGramReceiver.ProcessUnencodedSkipGrams();
        for (UnencodedSkipGram unencodedSkipGram : payload) {
            for (EncodedSkipGram encodedSkipGram : unencodedSkipGram.extractEncodedSkipGrams()) {
                responsibleEndpoint.tell(new SkipGramReceiver.ProcessEncodedSkipGram(encodedSkipGram), this.self());
            }
            message.getSkipGrams().add(unencodedSkipGram);
        }
        responsibleEndpoint.tell(message, this.self());
        this.skipGramsByResponsibleWordEndpoint.remove(responsibleEndpoint);
    }

    private void handle(SkipGramReceiver.RequestNextSkipGramChunk message) {
        // TODO: Implement tracking of file pointer (corpusPartition) for each node and send next chunk of relevant
        // skip grams to requesting node
    }
}
