package com.cisco.wap;

import com.cisco.wap.config.ClusterManager;
import com.cisco.wap.config.ConfigManager;
import com.cisco.wap.config.VoldemortConfig;
import com.cisco.wap.exception.VoldemortConfigError;
import com.cisco.wap.route.ConsistentHashRouter;
import com.cisco.wap.route.VoldemortNode;
import com.cisco.wap.server.ServerOptions;
import com.cisco.wap.server.VoldemortServiceImpl;
import com.google.common.collect.Maps;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class VoldemortServer {
    private static Logger logger = LoggerFactory.getLogger(VoldemortServer.class);
    public static void main(String[] args) {
        ServerOptions serverOptions = new ServerOptions(
                new String[] {"c"},
                new String[] {"confdir"},
                new String[] {"Configuration Directory"});
        Map<String, String>  options = serverOptions.parseOptions("VoldemortServer", args);
        String path = Objects.nonNull(options.get("confdir")) ? options.get("confdir") : ".";
        try {
            VoldemortConfig config = ConfigManager.getConfig(path);
            int nodeId = config.getNodeId();
            Map<Integer, VoldemortNode> voldemortNodes = ClusterManager.getConfig(path);
            VoldemortNode self = getPort(nodeId, voldemortNodes);

            List<VoldemortNode> allNodes = voldemortNodes.entrySet().stream()
                    .map(i -> i.getValue()).collect(Collectors.toList());
            ConsistentHashRouter<VoldemortNode> router = new ConsistentHashRouter<>(allNodes);

            Map<VoldemortNode, ManagedChannel> channels = Maps.newConcurrentMap();
            voldemortNodes.values().stream().forEach(node -> {
                ManagedChannel channel = ManagedChannelBuilder.forAddress(node.getAddress(), node.getPort())
                        .usePlaintext()
                        .build();
                channels.put(node, channel);
            });

            Server server = ServerBuilder.forPort(self.getPort())
                    .addService(new VoldemortServiceImpl(self, router, channels))
                    .build();
            server.start();
            logger.info(String.format("Voldemort Server is started at port %d.", self.getPort()));
            server.awaitTermination();
        } catch (IOException | InterruptedException e) {
            logger.error("Voldemort Server is stopped, due to: ", e);
        } catch (VoldemortConfigError fe) {
            logger.error("Voldemort Server is stopped, due to: ", fe);
        }
    }

    private static VoldemortNode getPort(int nodeId, Map<Integer, VoldemortNode> voldemortNodes) {
        VoldemortNode self = voldemortNodes.get(nodeId);
        Objects.requireNonNull(self, "Self should be an existing node.");
        return self;
    }

}
