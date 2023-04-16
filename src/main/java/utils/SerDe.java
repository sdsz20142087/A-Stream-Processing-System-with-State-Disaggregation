package utils;

import com.google.protobuf.ByteString;

public interface SerDe<T> {
    T deserializeIn(ByteString bs);
    ByteString serializeOut(T t);
}