package de.hpi.rdse.jujo.actors.testUtilities;

public class TestUtilities {

    public static String generateASCIIString(int numberOfBytes) {
        StringBuilder stringBuilder = new StringBuilder();
        int insertSpace = 1;
        while (stringBuilder.length() < numberOfBytes) {
            for (int i = 0; i < insertSpace; ++i) {
                stringBuilder.append('a');

                if (stringBuilder.length() >= numberOfBytes) {
                    break;
                }
            }

            if (stringBuilder.length() >= numberOfBytes) {
                break;
            }

            stringBuilder.append(' ');
            insertSpace = Math.max((insertSpace + 1) % 4, 1);
        }
        return  stringBuilder.toString();
    }
}
