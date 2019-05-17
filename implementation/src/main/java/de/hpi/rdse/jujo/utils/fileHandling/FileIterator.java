package de.hpi.rdse.jujo.utils.fileHandling;

import akka.util.ByteString;
import lombok.AllArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

@AllArgsConstructor
public class FileIterator implements Iterator<ByteString> {

    private static final Logger Log = LogManager.getLogger(FileIterator.class);

    private final FileInputStream fileStream;
    private final long chunkSize;
    private long readOffset;
    private long readLength;

    @Override
    public boolean hasNext() {
        return readLength > 0;
    }

    @Override
    public ByteString next() {
        try {
            fileStream.getChannel().position(readOffset);
            int bufferSize = (int) Math.min(chunkSize, readLength);
            byte[] buffer = new byte[bufferSize];
            long read = (long) fileStream.read(buffer);
            readOffset += read;
            readLength -= read;
            return ByteString.fromByteBuffer(ByteBuffer.wrap(buffer));
        } catch (IOException e) {
            Log.error("exception while reading from corpus file", e);
            readLength = 0;
            return ByteString.empty();
        }
    }
}