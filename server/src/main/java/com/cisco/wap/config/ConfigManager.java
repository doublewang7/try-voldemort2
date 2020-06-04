package com.cisco.wap.config;

import com.cisco.wap.exception.VoldemortConfigError;
import com.google.common.io.Resources;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;

public class ConfigManager {
    public static final String PROPERTIES = "server.properties";

    private ConfigManager(){}

    private static VoldemortConfig config;

    public static VoldemortConfig getConfig(String path) throws IOException, VoldemortConfigError {
        if(Objects.nonNull(config)) {
            return config;
        }
        Properties properties = loadConfig(path);
        config = new VoldemortConfig(properties);
        return config;
    }

    private static Properties loadConfig(String path) throws IOException {
        File file = new File(path, PROPERTIES);
        FileInputStream fileInputStream = new FileInputStream(file);
        Properties properties = new Properties();
        properties.load(fileInputStream);
        return properties;
    }
}
