package operators;

import com.google.protobuf.ByteString;

import java.io.Serializable;

public class SinkOperator extends BaseOperator implements Serializable {
    public SinkOperator() {
        super(null, null);
        this.setName("SinkOperator");
        this.setOpName("SinkOperator");
    }

    @Override
    protected void processElement(ByteString in, OutputSender outputSender) {
        System.out.println("--> "+in.toStringUtf8());
    }
}
