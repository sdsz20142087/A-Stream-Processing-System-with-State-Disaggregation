package operators.stateful;

import com.google.protobuf.ByteString;
import operators.BaseOperator;
import operators.OutputSender;
import pb.Tm;
import stateapis.ValueStateAccessor;
import utils.SerDe;

import java.io.Serializable;

public class SingleCountOperator extends BaseOperator implements Serializable {
    private transient ValueStateAccessor<Integer> cntAccesor;

    public SingleCountOperator(SerDe<String> out){
        super(null, out);
        this.setOpName("CountOperator");
    }

    @Override
    public void postInit() {
        cntAccesor = stateDescriptorProvider.getValueStateAccessor(this, "cnt", 0);
    }

    @Override
    protected void processElement(ByteString in, OutputSender outputSender) {
        //logger.info("cntAccesor.value() = " + cntAccesor.value());
        Integer cntVal = cntAccesor.value() + 1;
        cntAccesor.update(cntVal);
        String outMsg = "Count: " + (cntVal);
        outputSender.sendOutput(outMsg);
    }
}
