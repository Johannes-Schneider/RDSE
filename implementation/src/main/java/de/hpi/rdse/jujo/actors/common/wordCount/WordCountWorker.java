package de.hpi.rdse.jujo.actors.common.wordCount;

import akka.actor.Props;
import de.hpi.rdse.jujo.actors.common.AbstractReapedActor;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class WordCountWorker extends AbstractReapedActor {

    public static Props props() {
        return Props.create(WordCountWorker.class, WordCountWorker::new);
    }

    @NoArgsConstructor @AllArgsConstructor @Getter
    public static class CountWords implements Serializable {
        private static final long serialVersionUID = -319808125109589921L;
        private String chunk;
    }

    private WordCountWorker() { }

    @Override
    public Receive createReceive() {
        return defaultReceiveBuilder()
                .match(CountWords.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(CountWords message) {
        Map<String, Integer> wordCounts = new HashMap<>();
        for(String word : message.getChunk().split("\\s")) {
            if (word.isEmpty()) {
                continue;
            }
            String unifiedWord = Vocabulary.unify(word);
            if (wordCounts.computeIfPresent(unifiedWord, (key, value) -> value + 1) == null) {
                wordCounts.put(unifiedWord, 1);
            }
        }
        this.sender().tell(new WordCountCoordinator.WordsCounted(wordCounts), this.self());
    }
}
