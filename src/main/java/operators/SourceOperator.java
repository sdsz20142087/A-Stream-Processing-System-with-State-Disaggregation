package operators;

import com.google.protobuf.ByteString;
import pb.Op;


public class SourceOperator extends BaseOperator{
    public SourceOperator(Op.OperatorConfig config,ISource source) {
        super(config);
        // start a new thread to emit data and store them in the input queue
        new Thread(() -> {
            while (source.hasNext()) {
                ByteString data = source.next();
                Op.Msg msg = Op.Msg.newBuilder().setType(Op.Msg.MsgType.DATA).setData(data).build();
                addInput(msg);
            }
        }).start();
    }



    @Override
    // simply move whatever we have in the input queue to the output queue
    protected void processElement(ByteString in) {
        sendOutput(Op.Msg.newBuilder().setType(Op.Msg.MsgType.DATA).setData(in).build());
    }
}
