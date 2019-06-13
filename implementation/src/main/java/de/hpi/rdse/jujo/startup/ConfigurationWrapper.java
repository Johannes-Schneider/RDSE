package de.hpi.rdse.jujo.startup;

import com.typesafe.config.Config;

public class ConfigurationWrapper {

    public static final String MAXIMUM_MESSAGE_SIZE_KEY = "akka.remote.maximum-payload-bytes";

    private static Config config;

    public static Config initialize(Config config) {
        ConfigurationWrapper.config = config;
        return get();
    }

    public static Config get() {
        return config;
    }

    public static long getMaximumMessageSize() {
        return get().getBytes(MAXIMUM_MESSAGE_SIZE_KEY);
    }
}
