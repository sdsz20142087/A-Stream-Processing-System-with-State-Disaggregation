package operators;

import com.google.protobuf.ByteString;

public interface OutputSender {
    void sendOutput(Object o);
    public long getIngestTime();
}
