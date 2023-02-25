package operators;

import com.google.protobuf.ByteString;

public interface ISource{
    boolean hasNext();
    ByteString next();
}
