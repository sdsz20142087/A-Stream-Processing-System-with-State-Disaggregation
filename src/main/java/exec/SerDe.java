package exec;

import com.google.protobuf.ByteString;

public interface SerDe<T> {
    T deserialize(ByteString bs);
    ByteString serialize(T t);
}