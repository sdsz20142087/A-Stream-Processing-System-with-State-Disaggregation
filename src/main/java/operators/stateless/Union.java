package operators.stateless;

import com.google.protobuf.ByteString;
import operators.OutputSender;
import utils.SerDe;
import operators.BaseOperator;
import pb.Tm;

//public class Union<IN> extends BaseOperator {
//    public Union(SerDe<IN> serde) {
//        this.setName("UnionOperator-");
//    }
//
//    // what is this?
//    @Override
//    protected void processElement(ByteString in, OutputSender outputSender) {
//        outputSender.sendOutput(Tm.Msg.newBuilder().setType(Tm.Msg.MsgType.DATA).setData(in));
//    }
//
//    @Override
//    public void run(){
//        super.run();
//    }
//}
