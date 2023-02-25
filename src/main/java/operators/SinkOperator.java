package operators;

import com.google.protobuf.ByteString;
import pb.Op;

public class SinkOperator extends BaseOperator{
    public SinkOperator(Op.OperatorConfig config) {
        super(config);
    }

    @Override
    protected void processElement(ByteString in) {
        System.out.println("--> "+in.toStringUtf8());
    }
}
