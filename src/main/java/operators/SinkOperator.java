package operators;

import com.google.protobuf.ByteString;
import pb.Op;

import java.io.Serializable;

public class SinkOperator extends BaseOperator implements Serializable {
    public SinkOperator(Op.OperatorConfig config) {
        super(config);
    }

    @Override
    protected void processElement(ByteString in) {
        System.out.println("--> "+in.toStringUtf8());
    }
}
