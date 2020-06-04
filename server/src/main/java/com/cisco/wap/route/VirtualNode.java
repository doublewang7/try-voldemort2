package com.cisco.wap.route;

public class VirtualNode<T extends Node> implements Node {
    private final T physicalNode;
    private final int replicaIndex;

    public VirtualNode(T physicalNode, int replicaIndex) {
        this.physicalNode = physicalNode;
        this.replicaIndex = replicaIndex;
    }

    @Override
    public String getKey() {
        return physicalNode.getKey() + "-" + replicaIndex;
    }

    public boolean isVirtualOf(T that) {
        return physicalNode.equals(that);
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public T virtualOf() {
        return physicalNode;
    }

    @Override
    public String toString() {
        return "VirtualNode{" +
                "physicalNode=" + physicalNode.toString() +
                ", replicaIndex=" + replicaIndex +
                '}';
    }
}
