package com.cisco.wap.utils.codec;

import com.google.common.base.Strings;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Objects;

public class StringCodec implements Codec {
    private static final Charset charset = Charset.forName("UTF-8");

    @Override
    public byte[] encode(String str) {
        if(Strings.isNullOrEmpty(str)) {
            return new byte[0];
        }
        ByteBuffer buffer = charset.encode(str);
        byte[] ret = new byte[buffer.remaining()];
        buffer.get(ret);
        return ret;
    }

    @Override
    public String decode(byte[] bytes) {
        if(Objects.isNull(bytes)) {
            return "";
        }
        return charset.decode(ByteBuffer.wrap(bytes)).toString();
    }
}