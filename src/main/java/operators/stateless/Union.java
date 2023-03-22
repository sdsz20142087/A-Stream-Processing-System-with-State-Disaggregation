package operators.stateless;

import com.google.protobuf.ByteString;
import utils.SerDe;
import operators.BaseOperator;
import pb.Tm;

public class Union<IN> extends BaseOperator {
    public Union(SerDe<IN> serde) {
        this.setName("UnionOperator-");
    }

    @Override
    protected void processElement(ByteString in) {
        sendOutput(Tm.Msg.newBuilder().setType(Tm.Msg.MsgType.DATA).setData(in));
    }

    @Override
    public void run(){
        super.run();
    }
}
