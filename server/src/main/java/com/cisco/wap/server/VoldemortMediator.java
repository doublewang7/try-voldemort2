package com.cisco.wap.server;

import com.cisco.wap.config.VoldemortConfig;
import com.cisco.wap.route.ConsistentHashRouter;
import com.cisco.wap.route.VoldemortNode;
import com.google.common.collect.Maps;
import io.grpc.ManagedChannel;

import java.util.Map;

public class VoldemortMediator {
    private ConsistentHashRouter<VoldemortNode> router;
    private VoldemortConfig config;
    private Map<VoldemortNode, ManagedChannel> channels;

    public ConsistentHashRouter<VoldemortNode> getRouter() {
        return router;
    }

    public void setRouter(ConsistentHashRouter<VoldemortNode> router) {
        this.router = router;
    }

    public VoldemortConfig getConfig() {
        return config;
    }

    public void setConfig(VoldemortConfig config) {
        this.config = config;
    }

    public Map<VoldemortNode, ManagedChannel> getChannels() {
        return channels;
    }

    public void setChannels(Map<VoldemortNode, ManagedChannel> channels) {
        this.channels = Maps.newConcurrentMap();
        this.channels.putAll(channels);
    }
}
