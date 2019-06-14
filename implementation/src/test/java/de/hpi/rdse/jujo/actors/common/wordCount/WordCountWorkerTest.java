package de.hpi.rdse.jujo.actors.common.wordCount;

import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import de.hpi.rdse.jujo.actors.AbstractBaseTest;
import de.hpi.rdse.jujo.utils.Utility;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class WordCountWorkerTest extends AbstractBaseTest {

    @Test
    public void testCountWords() {
        new TestKit(actorSystem) {{
            ActorRef wordCountWorker = actorSystem.actorOf(WordCountWorker.props());

            String[] testSet = {"Dies", "ist", "ein", "Satz", "mit", "vielen", "Wörtern,", "wie", "Satz"};

            Map<String, Integer> expectedResult = new HashMap<String, Integer>() {{
                put("dies", 1);
                put("ist", 1);
                put("ein", 1);
                put("satz", 2);
                put("mit", 1);
                put("vielen", 1);
                put("wörtern,", 1);
                put("wie", 1);
            }};

            for (byte delimiter : Utility.DELIMITERS) {
                byte[] delimiters = {delimiter};
                wordCountWorker.tell(
                        new WordCountWorker.CountWords(String.join(Vocabulary.decode(delimiters), testSet)),
                        this.getRef());

                WordCountCoordinator.WordsCounted result = this.expectMsgClass(
                        WordCountCoordinator.WordsCounted.class);

                assertThat(result.getWordCounts()).isEqualTo(expectedResult);
            }
        }};
    }
}
