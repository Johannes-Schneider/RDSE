package de.hpi.rdse.jujo.fileHandling;

import de.hpi.rdse.jujo.actors.testUtilities.TestUtilities;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class FilePartitionerTest {

    private String createTestFile(int numberOfBytes) throws IOException {

        String fileContent = TestUtilities.generateASCIIString(numberOfBytes);

        UUID fileName = UUID.randomUUID();
        File tmpFile = File.createTempFile(fileName.toString(), "");
        Files.write(tmpFile.toPath(), fileContent.getBytes());
        return tmpFile.getPath();
    }

    @Test
    public void testPartitioningForEvenNumberOfBytesTwoPartitions() throws IOException {
        String testFilePath = createTestFile(100);
        FilePartitioner partitioner = new FilePartitioner(testFilePath, 2);
        FilePartition firstPartition = partitioner.getNextPartition();
        FilePartition secondPartition = partitioner.getNextPartition();
        // Length
        assertThat(firstPartition.getReadLength()).isGreaterThanOrEqualTo(50);
        assertThat(secondPartition.getReadLength()).isLessThanOrEqualTo(50);
        assertThat(firstPartition.getReadLength() + secondPartition.getReadLength()).isEqualTo(100);
        // Offsets
        assertThat(firstPartition.getReadOffset()).isZero();
        assertThat(secondPartition.getReadOffset()).isEqualTo(firstPartition.getReadLength());
        assertThat(secondPartition.getReadOffset() + secondPartition.getReadLength()).isEqualTo(100);
        // Whitespace as delimiter
        FileInputStream testFileInputStream = new FileInputStream(testFilePath);
        testFileInputStream.getChannel().position(secondPartition.getReadOffset());
        assertThat(testFileInputStream.read()).isEqualTo((byte) 0x20);
    }

    @Test
    public void testPartitioningForOddNumberOfBytesTwoPartitions() throws IOException {
        String testFilePath = createTestFile(133);
        FilePartitioner partitioner = new FilePartitioner(testFilePath, 2);
        FilePartition firstPartition = partitioner.getNextPartition();
        FilePartition secondPartition = partitioner.getNextPartition();
        // Length
        assertThat(firstPartition.getReadLength()).isGreaterThanOrEqualTo(67);
        assertThat(secondPartition.getReadLength()).isLessThanOrEqualTo(66);
        assertThat(firstPartition.getReadLength() + secondPartition.getReadLength()).isEqualTo(133);
        // Offsets
        assertThat(firstPartition.getReadOffset()).isZero();
        assertThat(secondPartition.getReadOffset()).isEqualTo(firstPartition.getReadLength());
        assertThat(secondPartition.getReadOffset() + secondPartition.getReadLength()).isEqualTo(133);
        // Whitespace as delimiter
        FileInputStream testFileInputStream = new FileInputStream(testFilePath);
        testFileInputStream.getChannel().position(secondPartition.getReadOffset());
        assertThat(testFileInputStream.read()).isEqualTo((byte) 0x20);
    }

    @Test
    public void testPartitioningForEvenNumberOfBytesThreePartitions() throws IOException {
        String testFilePath = createTestFile(100);
        FilePartitioner partitioner = new FilePartitioner(testFilePath, 3);
        FilePartition firstPartition = partitioner.getNextPartition();
        FilePartition secondPartition = partitioner.getNextPartition();
        FilePartition thirdPartition = partitioner.getNextPartition();
        // Length
        assertThat(firstPartition.getReadLength()).isGreaterThanOrEqualTo(33);
        assertThat(secondPartition.getReadLength()).isGreaterThanOrEqualTo(33);
        assertThat(thirdPartition.getReadLength()).isLessThanOrEqualTo(33);
        assertThat(firstPartition.getReadLength() + secondPartition.getReadLength() + thirdPartition.getReadLength()).isEqualTo(100);
        // Offsets
        assertThat(firstPartition.getReadOffset()).isZero();
        assertThat(secondPartition.getReadOffset()).isEqualTo(firstPartition.getReadLength());
        assertThat(thirdPartition.getReadOffset()).isEqualTo(secondPartition.getReadOffset() + secondPartition.getReadLength());
        assertThat(thirdPartition.getReadOffset() + thirdPartition.getReadLength()).isEqualTo(100);
        // Whitespace as delimiter
        FileInputStream testFileInputStream = new FileInputStream(testFilePath);
        testFileInputStream.getChannel().position(secondPartition.getReadOffset());
        assertThat(testFileInputStream.read()).isEqualTo((byte) 0x20);
        testFileInputStream.getChannel().position(thirdPartition.getReadOffset());
        assertThat(testFileInputStream.read()).isEqualTo((byte) 0x20);
    }

    @Test
    public void testPartitioningForOddNumberOfBytesThreePartitions() throws IOException {
        String testFilePath = createTestFile(133);
        FilePartitioner partitioner = new FilePartitioner(testFilePath, 3);
        FilePartition firstPartition = partitioner.getNextPartition();
        FilePartition secondPartition = partitioner.getNextPartition();
        FilePartition thirdPartition = partitioner.getNextPartition();
        // Length
        assertThat(firstPartition.getReadLength()).isGreaterThanOrEqualTo(44);
        assertThat(secondPartition.getReadLength()).isGreaterThanOrEqualTo(44);
        assertThat(thirdPartition.getReadLength()).isLessThanOrEqualTo(44);
        assertThat(firstPartition.getReadLength() + secondPartition.getReadLength() + thirdPartition.getReadLength()).isEqualTo(133);
        // Offsets
        assertThat(firstPartition.getReadOffset()).isZero();
        assertThat(secondPartition.getReadOffset()).isEqualTo(firstPartition.getReadLength());
        assertThat(thirdPartition.getReadOffset()).isEqualTo(secondPartition.getReadOffset() + secondPartition.getReadLength());
        assertThat(thirdPartition.getReadOffset() + thirdPartition.getReadLength()).isEqualTo(133);
        // Whitespace as delimiter
        FileInputStream testFileInputStream = new FileInputStream(testFilePath);
        testFileInputStream.getChannel().position(secondPartition.getReadOffset());
        assertThat(testFileInputStream.read()).isEqualTo((byte) 0x20);
        testFileInputStream.getChannel().position(thirdPartition.getReadOffset());
        assertThat(testFileInputStream.read()).isEqualTo((byte) 0x20);
    }
}
