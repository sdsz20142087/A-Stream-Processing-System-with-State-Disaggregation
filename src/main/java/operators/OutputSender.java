package operators;

import pb.Tm;

public interface OutputSender {
    void sendOutput(Tm.Msg.Builder msgBuilder);
}
