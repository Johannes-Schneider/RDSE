package de.hpi.rdse.jujo.training;

import akka.actor.RootActorPath;
import de.hpi.rdse.jujo.fileHandling.FileWordIterator;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import de.hpi.rdse.jujo.wordManagement.WordEndpointResolver;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class SkipGramProducer implements Iterator<List<UnencodedSkipGram>> {

    private static final Logger Log = LogManager.getLogger(SkipGramProducer.class);


    private final RootActorPath skipGramReceiver;
    private final FileWordIterator fileIterator;
    private final List<String> words = new ArrayList<>();
    private long epochStartTime = System.currentTimeMillis();
    private float lastPrintedProgress = 0.0f;
    @Getter
    private int currentEpoch = 0;


    public SkipGramProducer(RootActorPath skipGramReceiver, String localCorpusFilePath) throws FileNotFoundException {
        this.skipGramReceiver = skipGramReceiver;
        this.fileIterator = new FileWordIterator(localCorpusFilePath);
    }

    @Override
    public boolean hasNext() {
        return this.currentEpoch + 1 < Word2VecModel.getInstance().getConfiguration().getNumberOfEpochs() ||
                this.fileIterator.hasNext();
    }

    @Override
    public List<UnencodedSkipGram> next() {

        if (!this.hasNext()) {
            return Collections.emptyList();
        }

        this.startNextEpochIfAtEndOfEpoch();

        this.words.addAll(Arrays.asList(this.fileIterator.next()));

        float progress = this.fileIterator.progress();
        if (Math.abs(progress - this.lastPrintedProgress) >= 1.0f) {
            Log.info(String.format("##################### [%s] Epoch %d - %f %% #####################",
                    this.skipGramReceiver, this.currentEpoch, progress * 100));
            this.lastPrintedProgress = progress;
        }

        List<String> wordsForSkipGramProduction = this.words.subList(0,
                this.words.size() - Word2VecModel.getInstance().getConfiguration().getWindowSize());

        List<UnencodedSkipGram> skipGrams = new ArrayList<>();
        for (int i = 0; i < wordsForSkipGramProduction.size(); ++i) {
            if (WordEndpointResolver.getInstance().resolve(this.words.get(i)).path().root() != this.skipGramReceiver) {
                continue;
            }

            UnencodedSkipGram skipGram = this.createSkipGramForWordAt(i);
            if (skipGram.isEmpty()) {
                continue;
            }

            skipGrams.add(skipGram);
        }
        wordsForSkipGramProduction.clear();
        return skipGrams;
    }

    private void startNextEpochIfAtEndOfEpoch() {
        if (this.fileIterator.hasNext()) {
            return;
        }
        Log.info(String.format("##################### [%s] Epoch %d finished after %f min #####################",
                this.skipGramReceiver, this.currentEpoch,
                (System.currentTimeMillis() - this.epochStartTime) / (double)(1000 * 60)));
        this.currentEpoch++;
        this.fileIterator.reset();

        this.epochStartTime = System.currentTimeMillis();
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
}
