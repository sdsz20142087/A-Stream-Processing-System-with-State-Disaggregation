package operators;

import com.google.protobuf.ByteString;
import exec.SerDe;
import pb.Op;


public class SourceOperator<T> extends BaseOperator{
    public SourceOperator(Op.OperatorConfig config,ISource<T> source, SerDe<T> serde) {
        super(config);
        // start a new thread to emit data and store them in the input queue
        // TODO: the design is NOT finalized yet! We are not yet sure how to handle
        // stream item serialization/de-serialization
        new Thread(() -> {
            while (source.hasNext()) {
                T data = source.next();
                ByteString bs = serde.serialize(data);
                Op.Msg msg = Op.Msg.newBuilder().setType(Op.Msg.MsgType.DATA).setData(bs).build();
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
