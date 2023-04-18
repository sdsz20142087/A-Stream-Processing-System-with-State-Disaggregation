package operators.stateful;

import com.google.protobuf.ByteString;
import operators.BaseOperator;
import operators.OutputSender;
import pb.Tm;
import stateapis.IDataflowDeque;
import stateapis.ListStateAccessor;
import utils.SerDe;

import java.io.Serializable;

public class SlidingWindowSumOperator extends BaseOperator implements Serializable {
    private transient ListStateAccessor<Double> windowStateAccessor;
    private int windowSize;

    public SlidingWindowSumOperator(SerDe serdeIn, SerDe serdeOut) {
        super(serdeIn, serdeOut);
    }

    @Override
    protected void processElement(Tm.Msg msg, OutputSender outputSender) {
        IDataflowDeque<Double> window = (IDataflowDeque<Double>) windowStateAccessor.value();

        ByteString in = msg.getData();

        // Convert ByteString to Double
        Double input = Double.parseDouble(in.toStringUtf8());

        // Add the input value to the window
        window.addLast(input);

        // Remove the oldest value if the window size is exceeded
        if (window.size() > windowSize) {
            window.removeFirst();
        }

        // Calculate the sum of the values in the window
        double sum = 0.0;
        for (int i = 0; i < window.size(); i++) {
            sum += window.removeFirst();
            window.addLast(sum);
        }

        // Convert sum to ByteString and send it as output
        ByteString out = ByteString.copyFromUtf8(Double.toString(sum));
        outputSender.sendOutput(Tm.Msg.newBuilder().setType(Tm.Msg.MsgType.DATA).setData(out).build());
    }
}
