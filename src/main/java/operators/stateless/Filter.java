package operators.stateless;

import java.io.Serializable;
import java.util.function.Predicate;

import com.google.protobuf.ByteString;
import operators.OutputSender;
import utils.SerDe;
import operators.BaseOperator;

public class Filter<T> extends BaseOperator implements Serializable {
    private Predicate<T> predicate;
    public Filter(SerDe<T> serde, Predicate<T> UDFpredicate) {
        super(serde,serde);
        this.setOpName("FilterOperator");
        this.predicate = UDFpredicate;
    }
    @Override
    public void run(){
        super.run();
    }
    @Override
    protected void processElement(ByteString in, OutputSender outputSender) {
        T data = (T) serdeIn.deserializeIn(in);
        if (predicate.test(data)) {
            outputSender.sendOutput(data);
        }
    }
}