package de.hpi.rdse.jujo.actors.common;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rdse.jujo.fileHandling.FilePartionIterator;
import de.hpi.rdse.jujo.utils.Utility;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class WordCountWorker extends AbstractReapedActor {

    public static Props props(ActorRef supervisor) {
        return Props.create(WordCountWorker.class, () -> new WordCountWorker(supervisor));
    }

    private final ActorRef supervisor;
    private final Map<String, Long> wordCount = new HashMap<>();
    private final ByteBuffer remainingBytes;

    private WordCountWorker(ActorRef supervisor) {
        this.supervisor = supervisor;
        this.remainingBytes = ByteBuffer.allocate(FilePartionIterator.chunkSize() * 2);
    }

    @Override
    public Receive createReceive() {
        return this.defaultReceiveBuilder()
                .match(WorkerCoordinator.ProcessCorpusChunk.class, this::handle)
                .match(WorkerCoordinator.CorpusTransferCompleted.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(WorkerCoordinator.ProcessCorpusChunk message) {
        byte[] chunkBytes = new byte[message.getCorpusChunk().asByteBuffer().capacity()];
        message.getCorpusChunk().asByteBuffer().get(chunkBytes);
        int lastDelimiterIndex = Utility.LastIndexOfDelimiter(chunkBytes);
        this.remainingBytes.put(chunkBytes, 0, lastDelimiterIndex);
        this.remainingBytes.position(0);
        String decodedChunk = Vocabulary.decode(this.remainingBytes);
        this.remainingBytes.clear();
        this.remainingBytes.put(chunkBytes, lastDelimiterIndex, chunkBytes.length - lastDelimiterIndex);
        this.countWords(decodedChunk.split("\\s"));
    }

    private void handle(WorkerCoordinator.CorpusTransferCompleted message) {
        String decodedChunk = Vocabulary.decode(this.remainingBytes);
        this.remainingBytes.clear();
        this.countWords(decodedChunk.split("\\s"));
        this.supervisor.tell(new Subsampler.WordsCounted(this.wordCount), this.self());
    }

    private void countWords(String[] words) {
        for(String word: words) {
            if (this.wordCount.computeIfPresent(word, (key, count) -> count + 1) == null) {
                this.wordCount.put(word, 1L);
            }
        }
    }
}
