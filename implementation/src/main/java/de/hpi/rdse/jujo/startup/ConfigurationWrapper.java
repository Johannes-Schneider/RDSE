package de.hpi.rdse.jujo.startup;

import com.typesafe.config.Config;

public class ConfigurationWrapper {

    private static Config config;

    public static Config initialize(Config config) {
        ConfigurationWrapper.config = config;
        return get();
    }

    public static Config get() {
        return config;
    }

    public static long getMaximumMessageSize() {
        return get().getBytes("akka.remote.maximum-payload-bytes");
    }
}
