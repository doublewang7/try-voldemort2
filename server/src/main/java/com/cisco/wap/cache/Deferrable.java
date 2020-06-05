package com.cisco.wap.cache;

public interface Deferrable {
    byte[] toByte();
    <T extends Deferrable> T fromByte(byte[] arr);
}
