package utils;

import com.google.protobuf.ByteString;

import java.io.Serializable;

public class StringSerde implements SerDe<String>, Serializable {
    @Override
    public String deserialize(ByteString bs) {
        return bs.toString();
    }

    @Override
    public ByteString serialize(String obj) {
        return ByteString.copyFromUtf8(obj);
    }
}
