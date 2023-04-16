package utils;

import com.google.protobuf.ByteString;

import java.io.Serializable;

public class StringSerde implements SerDe<String>, Serializable {
    @Override
    public String deserializeIn(ByteString bs) {
        return bs.toStringUtf8();
    }

    @Override
    public ByteString serializeOut(String obj) {
        return ByteString.copyFromUtf8(obj);
    }
}
