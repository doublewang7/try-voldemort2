package com.cisco.wap.config;

import com.cisco.wap.exception.VoldemortConfigError;

import java.util.Objects;
import java.util.Properties;

public interface ConfigConstant {
    String NODE_ID = "node.id";
    String BIG_QUEUE_STORE_PATH = "queue.store.path";

    static int getIntValue(Properties properties, String name) throws VoldemortConfigError {
        String property = properties.getProperty(name);
        if(Objects.isNull(property)) {
            throw new VoldemortConfigError("missing " + name);
        }
        return Integer.parseInt(property);
    }

    static String getStringValue(Properties properties, String name) throws VoldemortConfigError {
        String property = properties.getProperty(name);
        if(Objects.isNull(property)) {
            throw new VoldemortConfigError("missing " + name);
        }
        return property;
    }
}
