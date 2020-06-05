package com.cisco.wap.config;

import com.cisco.wap.exception.VoldemortConfigError;

import java.util.Properties;

import static com.cisco.wap.config.ConfigConstant.*;

public class VoldemortConfig {
    private Properties properties;
    private int nodeId;
    private String bigQueueStorePath;

    public VoldemortConfig(Properties properties) throws VoldemortConfigError {
        this.properties = properties;
        initConfig();
    }

    private void initConfig() throws VoldemortConfigError {
        nodeId = getIntValue(properties, NODE_ID);
        bigQueueStorePath = getStringValue(properties, BIG_QUEUE_STORE_PATH);
     }

    public int getNodeId() {
        return nodeId;
    }

    public String getBigQueueStorePath() {
        return bigQueueStorePath;
    }
}
