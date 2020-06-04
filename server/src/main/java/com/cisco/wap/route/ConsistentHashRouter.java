package com.cisco.wap.route;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsistentHashRouter<T extends Node> {
    private SortedMap<Long, VirtualNode<T>> hashRing = Maps.newTreeMap();
    private final HashFunction hashFunction = Hashing.murmur3_32();
    private final int replicaFact;

    public ConsistentHashRouter(Collection<T> nodes) {
        this(nodes, 2);
    }

    public ConsistentHashRouter(Collection<T> nodes, int replicaFact) {
        this.replicaFact = replicaFact;
        nodes.forEach(node -> addNode(node));
    }

    public T routeRequest(String key) {
        long objHash = hash(key);
        SortedMap<Long, VirtualNode<T>> tails = hashRing.tailMap(objHash);
        /**
         *  if tails is empty, that represent the object hash value is exit the tail, then return head
         *  if tails has value, that represent then return the first of tails
         */
        Long nodeHash = tails.isEmpty() ? hashRing.firstKey() : tails.firstKey();
        return hashRing.get(nodeHash).virtualOf();
    }

    public void addNode(T node) {
        List<Integer> indexes = IntStream.range(0, replicaFact)
                .mapToObj(i -> Integer.valueOf(i)).collect(Collectors.toList());
        hashRing.values().forEach(virtualNode -> {
            if(virtualNode.isVirtualOf(node)) {
                indexes.remove(virtualNode.getReplicaIndex());
            }
        });
        for(Integer index : indexes) {
            VirtualNode<T> virtualNode = new VirtualNode<>(node, index);
            long hashValue = hash(virtualNode.getKey());
            hashRing.put(hashValue, virtualNode);
        }
    }

    public void removeNode(T node) {
        Iterator<VirtualNode<T>> iterator = hashRing.values().iterator();
        while(iterator.hasNext()) {
            VirtualNode<T> virtualNode = iterator.next();
            if(virtualNode.isVirtualOf(node)) {
                iterator.remove();
            }
        }
    }

    private long hash(String key) {
        HashCode hashCode = hashFunction.hashString(key, Charsets.UTF_8);
        return hashCode.padToLong();
    }

    public String describeHashRing() {
        List<String> strings = hashRing.entrySet().stream().map(each -> {
            StringBuffer sb = new StringBuffer();
            sb.append("position=").append(each.getKey())
                    .append(", ")
                    .append(each.getValue().virtualOf().toString());
            return sb.toString();
        }).collect(Collectors.toList());
        return Joiner.on(System.lineSeparator()).join(strings);
    }
}
