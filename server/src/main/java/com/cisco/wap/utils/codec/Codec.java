package com.cisco.wap.utils.codec;

public interface Codec {
    byte[] encode(String t);
    String decode(byte[] bytes);
}
