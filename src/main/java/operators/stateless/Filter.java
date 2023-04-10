package operators.stateless;

import java.io.Serializable;
import java.util.function.Predicate;

import com.google.protobuf.ByteString;
import operators.OutputSender;
import utils.SerDe;
import pb.Tm;
import operators.BaseOperator;

public class Filter<T> extends BaseOperator implements Serializable {
    private Predicate<T> predicate;
    private SerDe<T> serdeIn;
    public Filter(SerDe<T> serdeIn, Predicate<T> UDFpredicate) {
        super();
        this.setOpName("FilterOperator");
        this.predicate = UDFpredicate;
        this.serdeIn = serdeIn;
    }
    @Override
    public void run(){
        super.run();
    }
    @Override
    protected void processElement(ByteString in, OutputSender outputSender) {
        T data = serdeIn.deserialize(in);
        if (predicate.test(data)) {
            outputSender.sendOutput(Tm.Msg.newBuilder().setType(Tm.Msg.MsgType.DATA).setData(in));
        }
    }
}