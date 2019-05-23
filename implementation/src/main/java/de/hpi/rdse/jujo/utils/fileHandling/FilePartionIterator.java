package de.hpi.rdse.jujo.utils.fileHandling;

import akka.util.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class FilePartionIterator implements Iterator<ByteString> {

    private static final Logger Log = LogManager.getLogger(FilePartionIterator.class);

    private static final int READ_CHUNK_SIZE = 1024;

    private final FileInputStream inputStream;
    private final long readEnd;

    public FilePartionIterator(FilePartition filePartition, File file) throws IOException {
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
            return (int) Math.min(READ_CHUNK_SIZE, this.readEnd - this.inputStream.getChannel().position());
        } catch (IOException e) {
            Log.error("Unable to calculate next element size for network transfer.", e);
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