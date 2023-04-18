package operators.stateful;


import com.google.protobuf.ByteString;
import operators.BaseOperator;
import operators.OutputSender;
import operators.stateless.Map;
import pb.Tm;
import stateapis.MapStateAccessor;
import utils.SerDe;

import java.io.Serializable;

public class FrequencyCountOperator extends BaseOperator implements Serializable {
    private transient MapStateAccessor<Integer> frequencyMapStateAccessor;

    public FrequencyCountOperator(SerDe<String> out) {
        super(null, out);
        this.setOpName("FrequencyCountOperator");
    }

    @Override
    public void postInit() {
        frequencyMapStateAccessor = stateDescriptorProvider.getMapStateAccessor(this, "frequency");
    }

    @Override
    protected void processElement(Tm.Msg msg, OutputSender outputSender) {
        String inputElement = msg.getData().toStringUtf8();

        // Get the current frequency count
        Integer currentCount = frequencyMapStateAccessor.value().get(inputElement);

        // If the element is not in the map, set the count to 1
        if (currentCount == null) {
            currentCount = 1;
        } else {
            // Otherwise, increment the count by 1
            currentCount++;
        }

        // Update the map state
        frequencyMapStateAccessor.value().put(inputElement, currentCount);

        // Create output message
        String outMsg = "Element: " + inputElement + ", Frequency: " + currentCount;

        // Send output message
        outputSender.sendOutput(Tm.Msg.newBuilder()
                .setType(Tm.Msg.MsgType.DATA)
                .setData(ByteString.copyFromUtf8(outMsg)).build()
        );
    }

}