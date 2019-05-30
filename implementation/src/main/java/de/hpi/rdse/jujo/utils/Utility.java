package de.hpi.rdse.jujo.utils;

import com.google.common.primitives.Bytes;

public class Utility {

    private static final byte[] DELIMITERS = new byte[] {
            (byte) 0x09, // tab
            (byte) 0x0a, // new line
            (byte) 0x20, // space
    };

    public static int NextIndexOfDelimiter(byte[] bytes) {
        boolean delimiterFound = false;
        int nextIndex = bytes.length;
        for (byte delimiter : DELIMITERS) {
            int index = Bytes.indexOf(bytes, delimiter);
            if (index < 0) {
                continue;
            }
            delimiterFound = true;
            nextIndex = Math.min(nextIndex, index);
        }

        return delimiterFound ? nextIndex : -1;
    }

    public static int LastIndexOfDelimiter(byte[] bytes) {
        boolean delimiterFound = false;
        int lastIndex = -1;
        for (byte delimiter : DELIMITERS) {
            int index = Bytes.lastIndexOf(bytes, delimiter);
            if (index < 0) {
                continue;
            }
            delimiterFound = true;
            lastIndex = Math.max(lastIndex, index);
        }

        return delimiterFound ? lastIndex : bytes.length;
    }

}
