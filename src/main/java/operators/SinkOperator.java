package operators;

import com.google.protobuf.ByteString;
import pb.Tm;

import java.io.Serializable;

public class SinkOperator extends BaseOperator implements Serializable {
    public SinkOperator() {
        super();
        this.setName("SinkOperator");
    }

    @Override
    protected void processElement(ByteString in) {
        System.out.println("--> "+in.toStringUtf8());
    }
}
