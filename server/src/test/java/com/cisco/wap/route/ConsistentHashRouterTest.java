package com.cisco.wap.route;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import mockit.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.SortedMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsistentHashRouterTest {

    private ConsistentHashRouter router;

    @Before
    public void init() {
        List<VoldemortNode> nodes = IntStream.rangeClosed(0, 1).mapToObj(i -> {
            return new VoldemortNode(i, "127.0.0.1", 6666 + i);
        }).collect(Collectors.toList());
        router = new ConsistentHashRouter(nodes);
    }

    @Test
    public void testRingThenEachNodeHasTwoVirtualNode() {
        VoldemortNode expectedNodeId0 = new VoldemortNode(0, "127.0.0.1", 6666);
        VoldemortNode expectedNodeId1 = new VoldemortNode(1, "127.0.0.1", 6667);
        SortedMap<Long, VirtualNode<VoldemortNode>> ring = (SortedMap<Long, VirtualNode<VoldemortNode>>)
                Deencapsulation.getField(router, "hashRing");
        long actualNodeId0Replica = ring.values().stream()
                .filter(value -> value.isVirtualOf(expectedNodeId0)).count();
        long actualNodeId1Replica = ring.values().stream()
                .filter(value -> value.isVirtualOf(expectedNodeId1)).count();
        Assert.assertEquals(actualNodeId0Replica, 2);
        Assert.assertEquals(actualNodeId1Replica, 2);
    }

    @Test
    public void testAddNodeWhenExtendWithDefaultReplicaThenCountAsTwo() {
        SortedMap<Long, VirtualNode<VoldemortNode>> ring = (SortedMap<Long, VirtualNode<VoldemortNode>>)
                Deencapsulation.getField(router, "hashRing");
        VoldemortNode node = new VoldemortNode(2, "127.0.0.1", 6668);
        router.addNode(node);
        long actualNodeReplica = ring.values().stream()
                .filter(value -> value.isVirtualOf(node)).count();
        Assert.assertEquals(actualNodeReplica, 2);
    }

    @Test
    public void testRemoveNodeWhenWithDefaultReplicaThenCountAsZero() {
        SortedMap<Long, VirtualNode<VoldemortNode>> ring = (SortedMap<Long, VirtualNode<VoldemortNode>>)
                Deencapsulation.getField(router, "hashRing");
        VoldemortNode node = new VoldemortNode(3, "127.0.0.1", 6669);
        router.addNode(node);
        router.removeNode(node);
        long actualNodeReplica = ring.values().stream()
                .filter(value -> value.isVirtualOf(node)).count();
        Assert.assertEquals(actualNodeReplica, 0);
    }

    @Test
    public void testRouteNodeWhenHashValueLargeThanMaxThenGetFirstNode() {
        SortedMap<Long, VirtualNode<VoldemortNode>> ring = (SortedMap<Long, VirtualNode<VoldemortNode>>)
                Deencapsulation.getField(router, "hashRing");
        Optional<Long> max = ring.keySet().stream().max(Long::compare);
        long hashValue = max.get() + 1;
        String inputKey = "Request_key";

        List<VoldemortNode> nodes = IntStream.rangeClosed(0, 1).mapToObj(i -> {
            return new VoldemortNode(i, "127.0.0.1", 6666 + i);
        }).collect(Collectors.toList());
        MockUp<ConsistentHashRouter> mockUp = new MockUp<ConsistentHashRouter>() {
            @Mock
            private long hash(String key) {
                if(inputKey.equalsIgnoreCase(key)) {
                    return hashValue;
                }
                return Deencapsulation.invoke(router, "hash", key);
            }
        };
        ConsistentHashRouter mockInstance = mockUp.getMockInstance();
        Deencapsulation.setField(mockInstance,"hashRing", Maps.newTreeMap());
        Deencapsulation.setField(mockInstance,"hashFunction", Hashing.murmur3_32());
        Deencapsulation.setField(mockInstance,"replicaFact", 2);
        nodes.forEach(node->mockInstance.addNode(node));
        Node node = mockInstance.routeRequest(inputKey);
        SortedMap<Long, VirtualNode<VoldemortNode>> mockRing = (SortedMap<Long, VirtualNode<VoldemortNode>>)
                Deencapsulation.getField(mockInstance, "hashRing");
        VoldemortNode expectedNode = mockRing.get(mockRing.firstKey()).virtualOf();
        Assert.assertEquals(node, expectedNode);
    }

    @Test
    public void testRouteNodeWhenHashValueLessThenMinThenGetFirstNode() {
        SortedMap<Long, VirtualNode<VoldemortNode>> ring = (SortedMap<Long, VirtualNode<VoldemortNode>>)
                Deencapsulation.getField(router, "hashRing");
        Optional<Long> min = ring.keySet().stream().min(Long::compare);
        long hashValue = min.get() - 1;
        String inputKey = "Request_key";

        List<VoldemortNode> nodes = IntStream.rangeClosed(0, 1).mapToObj(i -> {
            return new VoldemortNode(i, "127.0.0.1", 6666 + i);
        }).collect(Collectors.toList());
        MockUp<ConsistentHashRouter> mockUp = new MockUp<ConsistentHashRouter>() {
            @Mock
            private long hash(String key) {
                if(inputKey.equalsIgnoreCase(key)) {
                    return hashValue;
                }
                return Deencapsulation.invoke(router, "hash", key);
            }
        };
        ConsistentHashRouter mockInstance = mockUp.getMockInstance();
        Deencapsulation.setField(mockInstance,"hashRing", Maps.newTreeMap());
        Deencapsulation.setField(mockInstance,"hashFunction", Hashing.murmur3_32());
        Deencapsulation.setField(mockInstance,"replicaFact", 2);
        nodes.forEach(node->mockInstance.addNode(node));
        Node node = mockInstance.routeRequest(inputKey);
        SortedMap<Long, VirtualNode<VoldemortNode>> mockRing = (SortedMap<Long, VirtualNode<VoldemortNode>>)
                Deencapsulation.getField(mockInstance, "hashRing");
        VoldemortNode expectedNode = mockRing.get(mockRing.firstKey()).virtualOf();
        Assert.assertEquals(node, expectedNode);
    }

    @Test
    public void testRouteNodeWhenHashValueLargeThanLastThenGetLastNode() {
        SortedMap<Long, VirtualNode<VoldemortNode>> ring = (SortedMap<Long, VirtualNode<VoldemortNode>>)
                Deencapsulation.getField(router, "hashRing");
        Optional<Long> max = ring.keySet().stream().max(Long::compare);
        long hashValue = max.get() - 1;
        String inputKey = "Request_key";

        List<VoldemortNode> nodes = IntStream.rangeClosed(0, 1).mapToObj(i -> {
            return new VoldemortNode(i, "127.0.0.1", 6666 + i);
        }).collect(Collectors.toList());
        MockUp<ConsistentHashRouter> mockUp = new MockUp<ConsistentHashRouter>() {
            @Mock
            private long hash(String key) {
                if(inputKey.equalsIgnoreCase(key)) {
                    return hashValue;
                }
                return Deencapsulation.invoke(router, "hash", key);
            }
        };
        ConsistentHashRouter mockInstance = mockUp.getMockInstance();
        Deencapsulation.setField(mockInstance,"hashRing", Maps.newTreeMap());
        Deencapsulation.setField(mockInstance,"hashFunction", Hashing.murmur3_32());
        Deencapsulation.setField(mockInstance,"replicaFact", 2);
        nodes.forEach(node->mockInstance.addNode(node));
        Node node = mockInstance.routeRequest(inputKey);
        SortedMap<Long, VirtualNode<VoldemortNode>> mockRing = (SortedMap<Long, VirtualNode<VoldemortNode>>)
                Deencapsulation.getField(mockInstance, "hashRing");
        VoldemortNode expectedNode = mockRing.get(mockRing.lastKey()).virtualOf();
        Assert.assertEquals(node, expectedNode);
    }

    @Test
    public void testHashThenAlwaysConsistent() {
        VoldemortNode node = new VoldemortNode(2, "127.0.0.1", 6668);
        IntStream.range(0,10).forEach(loop -> {
            List<VoldemortNode> nodes = IntStream.rangeClosed(0, 1).mapToObj(i -> {
                return new VoldemortNode(i, "127.0.0.1", 6666 + i);
            }).collect(Collectors.toList());
            ConsistentHashRouter router = new ConsistentHashRouter(nodes);
            Object hash = Deencapsulation.invoke(router, "hash", node.getKey());
            Assert.assertEquals(hash, 2021035872l);
        });
    }

    @Test
    public void testDescribeHashRing() {
        List<VoldemortNode> nodes = IntStream.rangeClosed(0, 3).mapToObj(i -> {
            return new VoldemortNode(i, randomIpAddress(), 6666);
        }).collect(Collectors.toList());
        ConsistentHashRouter router = new ConsistentHashRouter(nodes);
        System.out.println(router.describeHashRing());
    }

    private String randomIpAddress() {
        String SPOT = ".";
        Random r = new Random();
        List<Integer> list = IntStream.range(0, 4).mapToObj(i -> r.nextInt(256))
                .collect(Collectors.toList());
        return Joiner.on(SPOT).join(list);
    }
}