package de.hpi.rdse.jujo.actors;

import akka.actor.Props;
import akka.util.ByteString;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class WordCountWorker extends AbstractReapedActor {

    public static Props props(File storageFile) {
        return Props.create(WordCountWorker.class, () -> new WordCountWorker(storageFile));
    }

    @Builder @Getter @AllArgsConstructor
    static class ProcessCorpusChunk implements Serializable {
        private static final long serialVersionUID = -7118713465294264590L;
        final ByteString chunk;
    }

    private final File storageFile;
    private final Map<String, Long> wordCount = new HashMap<>();

    private WordCountWorker(File storageFile) {
        this.storageFile = storageFile;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ProcessCorpusChunk.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(ProcessCorpusChunk message) {

    }
}
