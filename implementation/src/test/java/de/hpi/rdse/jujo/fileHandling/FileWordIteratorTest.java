package de.hpi.rdse.jujo.fileHandling;

import de.hpi.rdse.jujo.TestUtilities;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

public class FileWordIteratorTest {

    private String createTestFile(int numberOfBytes) throws IOException {

        String fileContent = TestUtilities.generateASCIIString(numberOfBytes);

        UUID fileName = UUID.randomUUID();
        File tmpFile = File.createTempFile(fileName.toString(), "");
        Files.write(tmpFile.toPath(), fileContent.getBytes());
        return tmpFile.getPath();
    }
}
