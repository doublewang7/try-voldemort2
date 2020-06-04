package com.cisco.wap.config;

import com.cisco.wap.exception.VoldemortConfigError;

import java.util.Objects;
import java.util.Properties;

public interface ConfigConstant {
    String NODE_ID = "node.id";
    String SEVER_PORT = "server.port";

    static int getIntValue(Properties properties, String name) throws VoldemortConfigError {
        String property = properties.getProperty(name);
        if(Objects.nonNull(property)) {
            return Integer.parseInt(property);
        }
        throw new VoldemortConfigError("missing " + name);
    }
}
