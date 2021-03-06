package de.hpi.rdse.jujo.fileHandling;

import de.hpi.rdse.jujo.startup.ConfigurationWrapper;
import de.hpi.rdse.jujo.utils.Utility;
import de.hpi.rdse.jujo.wordManagement.Vocabulary;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

public class FileWordIterator implements Iterator<String[]> {

    private static final Logger Log = LogManager.getLogger(FileWordIterator.class);
    public static int chunkSize() {
        return (int) (ConfigurationWrapper.getMaximumMessageSize() * 0.01d);
    }

    private final FileInputStream inputStream;
    private final long fileLength;

    public FileWordIterator(String filePath) throws FileNotFoundException {
        File file = new File(filePath);
        this.fileLength = file.length();
        this.inputStream = new FileInputStream(file);
    }

    @Override
    public String[] next() {
        byte[] buffer = new byte[nextReadSize()];
        try {
            int readLength = this.inputStream.read(buffer);

            if (this.hasNext()) {
                int lastDelimiterIndex = Utility.lastIndexOfDelimiter(buffer);
                this.inputStream.skip((lastDelimiterIndex + 1) - buffer.length);
                readLength = lastDelimiterIndex;
            }

            return Vocabulary.decode(buffer, 0, readLength).split("\\s");
        } catch (IOException e) {
            Log.error("Unable to read from Filestream.", e);
            this.closeFileStream();
            return new String[0];
        }
    }

    @Override
    public boolean hasNext() {
        return nextReadSize() > 0;
    }

    public float progress() {
        try {
            return this.inputStream.getChannel().position() / (float)this.fileLength;
        } catch (IOException e) {
            return 0.0f;
        }
    }

    private int nextReadSize() {
        try {
            int nextReadSize = (int) Math.min(chunkSize(), this.fileLength - this.inputStream.getChannel().position());
            Log.debug(String.format("FileWordIterator's next read size is %d", nextReadSize));
            return nextReadSize;
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

    public void reset() {
        try {
            this.inputStream.getChannel().position(0);
        } catch (IOException e) {
            Log.error("Unable to reset FileWordIterator", e);
            this.closeFileStream();
        }
    }
}
