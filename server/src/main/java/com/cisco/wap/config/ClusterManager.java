package com.cisco.wap.config;

import com.cisco.wap.route.VoldemortNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ClusterManager {
    public static final String CLUSTER_JSON = "cluster.json";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Map<Integer, VoldemortNode> getConfig(String path) throws IOException {
        Map<Integer, VoldemortNode> maps = load(path).stream()
                .collect(Collectors.toMap(VoldemortNode::getId, Function.identity()));
        return maps;
    }

    private static List<VoldemortNode> load(String path) throws IOException {
        File file = new File(path, CLUSTER_JSON);
        FileInputStream fileInputStream = new FileInputStream(file);
        CollectionType collectionType = objectMapper.getTypeFactory()
                .constructCollectionType(List.class, VoldemortNode.class);
        return objectMapper.readValue(fileInputStream, collectionType);
    }
}
