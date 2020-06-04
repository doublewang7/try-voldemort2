package com.cisco.wap.config;

import com.cisco.wap.exception.VoldemortConfigError;

import java.util.Properties;

import static com.cisco.wap.config.ConfigConstant.*;

public class VoldemortConfig {
    private Properties properties;
    private int nodeId;
    private int serverPort;

    public VoldemortConfig(Properties properties) throws VoldemortConfigError {
        this.properties = properties;
        initConfig();
    }

    private void initConfig() throws VoldemortConfigError {
        nodeId = getIntValue(properties, NODE_ID);
     }

    public int getNodeId() {
        return nodeId;
    }

    public int getServerPort() {
        return serverPort;
    }
}
