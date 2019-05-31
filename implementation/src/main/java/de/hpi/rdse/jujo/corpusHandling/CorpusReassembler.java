package de.hpi.rdse.jujo.corpusHandling;

import akka.util.ByteString;
import de.hpi.rdse.jujo.utils.Utility;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;

public class CorpusReassembler {

    private final byte[] remainingBytes;
    private int remainingBytesLength = 0;

    public CorpusReassembler(int chunkSize) {
        this.remainingBytes = new byte[chunkSize];
    }

    public String decodeCorpusUntilNextDelimiter(ByteString chunk) {
        byte[] chunkBytes = new byte[chunk.asByteBuffer().capacity() + this.remainingBytesLength];
        System.arraycopy(this.remainingBytes, 0, chunkBytes, 0, this.remainingBytesLength);
        chunk.asByteBuffer().get(chunkBytes, this.remainingBytesLength, chunkBytes.length - this.remainingBytesLength);

        int lastDelimiterIndex = Utility.lastIndexOfDelimiter(chunkBytes);
        String decodedChunk = Vocabulary.decode(chunkBytes, 0, lastDelimiterIndex);

        this.remainingBytesLength = chunkBytes.length - lastDelimiterIndex;
        System.arraycopy(chunkBytes, lastDelimiterIndex, this.remainingBytes, 0, this.remainingBytesLength);
        return decodedChunk;
    }

    public String decodeRemainingCorpus() {
        String decodedChunk = Vocabulary.decode(this.remainingBytes, 0, this.remainingBytesLength);
        this.remainingBytesLength = 0;
        return decodedChunk;
    }
}
