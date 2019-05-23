package de.hpi.rdse.jujo.utils.fileHandling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilePartitioner {

    private static final Logger Log = LogManager.getLogger(FilePartitioner.class);

    public static final int SEEK_CHUNK_SIZE = 100;
    private static final String FILE_ENCODING = "UTF-8";

    private final File file;
    private final FileInputStream fileStream;
    private final long chunkSize;
    private long currentFileOffset = 0;
    private final Pattern whiteSpacePattern = Pattern.compile("\\s");

    public FilePartitioner(String filePath, int numberOfPartitions) {
        this.file = new File(filePath);
        this.fileStream = createFileStream();
        this.chunkSize = this.file.length() / numberOfPartitions;
    }

    private FileInputStream createFileStream() {
        try {
            return new FileInputStream(this.file);
        } catch (FileNotFoundException e) {
            Log.error("exception while creating file stream", e);
            return null;
        }
    }

    public FilePartition getNextPartition() {
        if (this.fileStreamIsAtEnd()) {
            this.closeFileStream();
            return FilePartition.empty();
        }

        try {
            long bufferSize = Math.min(this.chunkSize, this.file.length() - this.currentFileOffset);
            return getNextPartition(bufferSize);
        } catch (IOException e) {
            Log.error("exception while getting next partition: this should have not happened", e);
            this.closeFileStream();
            return FilePartition.empty();
        }
    }

    private boolean fileStreamIsAtEnd() {
        return this.currentFileOffset >= this.file.length();
    }

    private void closeFileStream() {
        try {
            this.fileStream.close();
        } catch (IOException e) {
            Log.error("exception while closing file stream: this should have not happened", e);
        }
    }

    private FilePartition getNextPartition(long bufferSize) throws IOException {
        bufferSize = Math.min(Integer.MAX_VALUE, bufferSize);

        this.fileStream.getChannel().position(this.currentFileOffset + bufferSize);
        long endOffset = getPositionOfNextWhiteSpace();

        FilePartition result = FilePartition.builder()
                .readOffset(this.currentFileOffset)
                .readLength(endOffset - this.currentFileOffset)
                .build();

        this.currentFileOffset = endOffset;

        if (this.fileStreamIsAtEnd()) {
            this.closeFileStream();
        }

        return result;
    }

    private long getPositionOfNextWhiteSpace() throws IOException {
        Matcher matcher;
        while (this.fileStream.getChannel().position() < this.file.length()) {
            this.fileStream.getChannel().position(fileStream.getChannel().position() + SEEK_CHUNK_SIZE);

            byte[] buffer = new byte[SEEK_CHUNK_SIZE];
            int actualRead = fileStream.read(buffer);
            String chunk = new String(buffer, 0, actualRead, Charset.forName(FILE_ENCODING));
            matcher = whiteSpacePattern.matcher(chunk);

            if (matcher.find()) {
                // finally found a white space
                return this.fileStream.getChannel().position() - SEEK_CHUNK_SIZE + matcher.end();
            }
        }
        return this.file.length();
    }
}
