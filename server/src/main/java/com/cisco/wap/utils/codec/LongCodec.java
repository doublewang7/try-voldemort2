package com.cisco.wap.utils.codec;

import com.google.common.base.Strings;

import java.util.Objects;

public class LongCodec implements Codec{
    @Override
    public byte[] encode(String str) {
        if(Strings.isNullOrEmpty(str)) {
            return new byte[0];
        }
        Long aLong = Long.parseLong(str);
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte)(aLong & 0xFF);
            aLong >>= 8;
        }
        return result;
    }

    @Override
    public String decode(byte[] bytes) {
        if(Objects.isNull(bytes)) {
            return "";
        }
        long result = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            result <<= Long.BYTES;
            result |= (bytes[i] & 0xFF);
        }
        return Long.toString(result);
    }
}
