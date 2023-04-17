package operators;

import com.google.protobuf.ByteString;
import pb.Tm;

public interface OutputSender {
    void sendOutput(Tm.Msg msg);
    long getIngestTime();
    void setIngestTime(long ingestTime);
}
