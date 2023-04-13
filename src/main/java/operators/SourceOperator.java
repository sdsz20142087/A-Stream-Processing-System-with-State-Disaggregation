package operators;

import com.google.protobuf.ByteString;
import utils.SerDe;
import pb.Tm;

import java.io.IOException;
import java.io.Serializable;


public class SourceOperator<T> extends BaseOperator implements Serializable {
    private ISource<T> source;
    private SerDe<T> serde;

    public SourceOperator(ISource<T> source, SerDe<T> serde) {
        super();
        this.setOpName("SourceOperator");
        this.source = source;
        this.serde = serde;
        // start a new thread to emit data and store them in the input queue
        // TODO: the design is NOT finalized yet! We are not yet sure how to handle
        // stream item serialization/de-serialization
    }

    @Override
    public void run(){

        new Thread(() -> {
            try {
                source.init();
            } catch (IOException e) {
                logger.fatal("source init failed: " + e.getMessage());
                System.exit(1);
            }
            while (true) {
                T data = source.next();
                ByteString bs = serde.serialize(data);
                Tm.Msg msg = Tm.Msg.newBuilder().setType(Tm.Msg.MsgType.DATA).setData(bs).build();
                inputQueue.add(msg);
            }
        }).start();
        super.run();
    }

    @Override
    // simply move whatever we have in the input queue to the output queue
    protected void processElement(ByteString in, OutputSender outputSender) {
        outputSender.sendOutput(Tm.Msg.newBuilder().setType(Tm.Msg.MsgType.DATA).setData(in));
    }
}
