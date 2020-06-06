package com.cisco.wap.cache;

import com.cisco.wap.utils.codec.Codec;
import com.cisco.wap.utils.codec.CodecFactory;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Objects;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeferredMessage implements Deferrable {
    private static final int KEY_HEAD_LENGTH = 4;
    private static final int VALUE_HEAD_LENGTH = 4;
    private static final String UTF_8 = "UTF-8";
    private final static Codec codec = CodecFactory.createInstance(CodecFactory.CodecType.StringCodec);

    private String key;
    private String value;

    @Override
    public byte[] toByte() {
        if(Objects.isNull(key) || Objects.isNull(value)) {
            return new byte[0];
        }
        byte[] keyBytes = key.getBytes(Charset.forName(UTF_8));
        byte[] valueBytes = value.getBytes(Charset.forName(UTF_8));

        int resultLength = KEY_HEAD_LENGTH + key.length() +
                VALUE_HEAD_LENGTH + value.length();
        byte[] bytesResult = new byte[resultLength];

        ByteBuffer buffer = ByteBuffer.wrap(bytesResult);
        buffer.putInt(key.length()).put(keyBytes);
        buffer.putInt(value.length()).put(valueBytes);
        return bytesResult;
    }

    @Override
    public DeferredMessage fromByte(byte[] arr) {
        ByteBuffer buffer = ByteBuffer.wrap(arr);
        // get key
        byte[] bytesKey = new byte[buffer.getInt()];
        buffer.get(bytesKey);
        String key = codec.decode(bytesKey);
        // get value
        byte[] bytesValue = new byte[buffer.getInt()];
        buffer.get(bytesValue);
        String value = codec.decode(bytesValue);

        this.setKey(key);
        this.setValue(value);
        return this;
    }
}
