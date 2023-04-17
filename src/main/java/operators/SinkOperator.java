package operators;

import com.google.protobuf.ByteString;
import pb.Tm;

import java.io.Serializable;

public class SinkOperator extends BaseOperator implements Serializable {
    public SinkOperator() {
        super(null, null);
        this.setName("SinkOperator");
        this.setOpName("SinkOperator");
    }

    @Override
    protected void processElement(Tm.Msg msg, OutputSender outputSender) {
        ByteString in = msg.getData();
        System.out.println("-->DATA "+in.toStringUtf8());
    }

    @Override
    protected void processWatermark(Tm.Msg msg, OutputSender outputSender) {
        ByteString in = msg.getData();
        System.out.println("-->WATERMARK: "+ msg.getIngestTime());
    }
}
