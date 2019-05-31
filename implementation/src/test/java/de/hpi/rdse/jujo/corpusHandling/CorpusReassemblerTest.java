package de.hpi.rdse.jujo.corpusHandling;

import akka.util.ByteString;
import de.hpi.rdse.jujo.actors.testUtilities.TestUtilities;
import org.junit.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class CorpusReassemblerTest {

    @Test
    public void testReassembleCorpus() {
        CorpusReassembler corpusReassembler = new CorpusReassembler(100);
        String payload = TestUtilities.generateASCIIString(170);
        ByteString firstChunk = ByteString.fromArray(payload.getBytes(), 0, 100);
        ByteString secondChunk = ByteString.fromArray(payload.getBytes(), 100, 70);
        String firstResult = corpusReassembler.decodeCorpusUntilNextDelimiter(firstChunk);
        String secondResult = corpusReassembler.decodeCorpusUntilNextDelimiter(secondChunk);
        String rest = corpusReassembler.decodeRemainingCorpus();

        assertThat(firstResult.length() + secondResult.length() + rest.length()).isEqualTo(170);
        assertThat(firstResult + secondResult + rest).isEqualTo(payload);
    }

    @Test
    public void testReassembleCorpusFor200() {
        CorpusReassembler corpusReassembler = new CorpusReassembler(100);
        String payload = TestUtilities.generateASCIIString(200);
        ByteString firstChunk = ByteString.fromArray(payload.getBytes(), 0, 100);
        ByteString secondChunk = ByteString.fromArray(payload.getBytes(), 100, 100);
        String firstResult = corpusReassembler.decodeCorpusUntilNextDelimiter(firstChunk);
        String secondResult = corpusReassembler.decodeCorpusUntilNextDelimiter(secondChunk);
        String rest = corpusReassembler.decodeRemainingCorpus();

        assertThat(firstResult.length() + secondResult.length() + rest.length()).isEqualTo(200);
        assertThat(firstResult + secondResult + rest).isEqualTo(payload);
    }
}
