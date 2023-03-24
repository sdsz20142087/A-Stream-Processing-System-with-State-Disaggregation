package operators.stateful;

import com.google.protobuf.ByteString;
import operators.BaseOperator;
import pb.Tm;
import stateapis.ValueStateAccessor;
import utils.SerDe;

import java.io.Serializable;

public class SingleCountOperator extends BaseOperator implements Serializable {
    private transient ValueStateAccessor<Integer> cntAccesor;
    private SerDe<String> outSerde;

    public SingleCountOperator(SerDe<String> out){
        super();
        this.setOpName("CountOperator");
        this.outSerde = out;
    }

    @Override
    public void postInit() {
        cntAccesor = stateDescriptorProvider.getValueStateAccessor(this, "cnt", 0);
    }

    @Override
    protected void processElement(ByteString in) {
        Integer cntVal = cntAccesor.value() + 1;
        cntAccesor.update(cntVal);
        String outMsg = "Count: " + (cntVal+1);

        ByteString out = outSerde.serialize(outMsg);
        sendOutput(Tm.Msg.newBuilder().setType(Tm.Msg.MsgType.DATA).setData(out));
    }
}
