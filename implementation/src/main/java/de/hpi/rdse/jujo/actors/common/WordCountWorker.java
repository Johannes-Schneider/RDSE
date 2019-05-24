package de.hpi.rdse.jujo.actors.common;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.google.common.primitives.Bytes;
import de.hpi.rdse.jujo.fileHandling.FilePartionIterator;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;

import java.nio.ByteBuffer;
import java.util.*;

public class WordCountWorker extends AbstractReapedActor {

    public static Props props(ActorRef supervisor) {
        return Props.create(WordCountWorker.class, () -> new WordCountWorker(supervisor));
    }

    private final ActorRef supervisor;
    private final Map<String, Long> wordCount = new HashMap<>();
    private final ByteBuffer remainingBytes = ByteBuffer.allocate(FilePartionIterator.CHUNK_SIZE * 2);

    private WordCountWorker(ActorRef supervisor) {
        this.supervisor = supervisor;
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
        int lastWhitespaceIndex = Bytes.lastIndexOf(chunkBytes, (byte) 0x20);
        this.remainingBytes.put(chunkBytes, 0, lastWhitespaceIndex);
        this.remainingBytes.position(0);
        String decodedChunk = Vocabulary.decode(this.remainingBytes);
        this.remainingBytes.clear();
        this.remainingBytes.put(chunkBytes, lastWhitespaceIndex + 1, chunkBytes.length - lastWhitespaceIndex - 1);
        this.countWords(decodedChunk.split(" "));
    }

    private void handle(WorkerCoordinator.CorpusTransferCompleted message) {
        String decodedChunk = Vocabulary.decode(this.remainingBytes);
        this.remainingBytes.clear();
        this.countWords(decodedChunk.split(" "));
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
