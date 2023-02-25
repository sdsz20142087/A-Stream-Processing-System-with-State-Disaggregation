package operators;

import com.google.protobuf.ByteString;
import exec.SerDe;
import pb.Op;

import java.io.Serializable;


public class SourceOperator<T> extends BaseOperator implements Serializable {
    private ISource<T> source;
    private SerDe<T> serde;

    public SourceOperator(Op.OperatorConfig config,ISource<T> source, SerDe<T> serde) {
        super(config);
        this.source = source;
        this.serde = serde;
        // start a new thread to emit data and store them in the input queue
        // TODO: the design is NOT finalized yet! We are not yet sure how to handle
        // stream item serialization/de-serialization
    }

    @Override
    protected void startGRPCServer(){
        // do nothing
        logger.info("source operator does not need to start a grpc server");
    }

    @Override
    public void run(){
        new Thread(() -> {
            try {
                source.init();
            } catch (Exception e) {
                logger.fatal("source init failed: " + e.getMessage());
                System.exit(1);
            }
            while (source.hasNext()) {
                T data = source.next();
                ByteString bs = serde.serialize(data);
                Op.Msg msg = Op.Msg.newBuilder().setType(Op.Msg.MsgType.DATA).setData(bs).build();
                addInput(msg);
            }
        }).start();
        super.run();
    }

    @Override
    // simply move whatever we have in the input queue to the output queue
    protected void processElement(ByteString in) {
        sendOutput(Op.Msg.newBuilder().setType(Op.Msg.MsgType.DATA).setData(in).build());
    }
}
