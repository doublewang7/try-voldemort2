package com.cisco.wap.utils.codec;

public class CodecFactory {
    private CodecFactory() {
    }

    public static Codec createInstance(CodecType type) {
        switch(type) {
            case LongCodec: return new LongCodec();
            case StringCodec: return new StringCodec();
            default: throw new RuntimeException("unsupported codec type");
        }
    }

    public enum CodecType {
        LongCodec, StringCodec;
    }
}
