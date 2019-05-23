package de.hpi.rdse.jujo.actors.common;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.util.ByteString;
import com.google.common.primitives.Bytes;
import de.hpi.rdse.jujo.actors.slave.CorpusReceiver;
import de.hpi.rdse.jujo.actors.slave.Slave;
import de.hpi.rdse.jujo.utils.fileHandling.FilePartionIterator;
import de.hpi.rdse.jujo.utils.fileHandling.FilePartitioner;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

public class WordCountWorker extends AbstractReapedActor {

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

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
        String decodedChunk = DEFAULT_CHARSET.decode(this.remainingBytes).toString();
        this.remainingBytes.clear();
        this.remainingBytes.put(chunkBytes, lastWhitespaceIndex + 1, chunkBytes.length - lastWhitespaceIndex - 1);
        this.countWords(decodedChunk.split(" "));
    }

    private void handle(WorkerCoordinator.CorpusTransferCompleted message) {
        String decodedChunk = DEFAULT_CHARSET.decode(this.remainingBytes).toString();
        this.remainingBytes.clear();
        this.countWords(decodedChunk.split(" "));
        this.supervisor.tell(new WordEndpoint.WordsCounted(this.wordCount), this.self());
    }

    private void countWords(String[] words) {
        for(String word: words) {
            if (this.wordCount.computeIfPresent(word, (key, count) -> count + 1) == null) {
                this.wordCount.put(word, 1L);
            }
        }
    }
}
