package com.cisco.wap.route;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@Data
@NoArgsConstructor
public class VoldemortNode implements Node {
    private int id;
    private String address;
    private int port;

    public VoldemortNode(int id, String address, int port) {
        this.id = id;
        this.address = address;
        this.port = port;
    }

    @Override
    public String getKey() {
        return address+"@"+port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VoldemortNode that = (VoldemortNode) o;
        return id == that.id &&
                port == that.port &&
                Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, address, port);
    }

    @Override
    public String toString() {
        return "VoldemortNode{" +
                "id=" + id +
                ", address='" + address + '\'' +
                ", port=" + port +
                '}';
    }
}
