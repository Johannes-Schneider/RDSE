package de.hpi.rdse.jujo.streaming;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StreamEndpoint {
    private static final Logger Log = LogManager.getLogger(StreamEndpoint.class);

    private static StreamEndpoint instance;

    public static void createInstance(StreamableSerializer serializer) {
        Log.info("Creating StreamEndpoint");

        if (StreamEndpoint.instance != null) {
            Log.error("Tried to create a second instance of the StreamEndpoint");
            return;
        }
        StreamEndpoint.instance = new StreamEndpoint(serializer);

        Log.info("Done creating StreamEndpoint");
    }

    public static StreamEndpoint getInstance() {
        return StreamEndpoint.instance;
    }

    private StreamEndpoint(StreamableSerializer serializer) {

    }
}
