package de.hpi.rdse.jujo.fileHandling;

import akka.util.ByteString;
import de.hpi.rdse.jujo.startup.ConfigurationWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class FilePartitionIterator implements Iterator<ByteString> {

    private static final Logger Log = LogManager.getLogger(FilePartitionIterator.class);
    public static int chunkSize() {
        return (int) (ConfigurationWrapper.getMaximumMessageSize() * 0.3d);
    }

    private final FileInputStream inputStream;
    private final long readEnd;

    public FilePartitionIterator(FilePartition filePartition, File file) throws IOException {
        this.readEnd = filePartition.getReadOffset() + filePartition.getReadLength();
        this.inputStream = new FileInputStream(file);
        this.inputStream.getChannel().position(filePartition.getReadOffset());
    }

    @Override
    public boolean hasNext() {
        return this.nextElementSize() > 0;
    }

    @Override
    public ByteString next() {
        byte[] buffer = new byte[this.nextElementSize()];
        try {
            this.inputStream.read(buffer);
            return ByteString.fromByteBuffer(ByteBuffer.wrap(buffer));
        } catch (IOException e) {
            Log.error("Unable to read from Filestream.", e);
            return ByteString.empty();
        }
    }

    private int nextElementSize() {
        try {
            return (int) Math.min(chunkSize(), this.readEnd - this.inputStream.getChannel().position());
        } catch (IOException e) {
            Log.error("Unable to calculate next element length for network transfer.", e);
            this.closeFileStream();
            return 0;
        }
    }

    private void closeFileStream() {
        try {
            this.inputStream.close();
        } catch (IOException e) {
            Log.warn("Unable to close Filestream", e);
        }
    }
}