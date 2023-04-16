package operators.stateless;

import com.google.protobuf.ByteString;
import operators.OutputSender;
import utils.SerDe;
import operators.BaseOperator;
import pb.Tm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FlatMap<T> extends BaseOperator implements Serializable{
    List<T> output= new ArrayList<>();
    public FlatMap(SerDe serDeIn, SerDe serDeOut) {
        super(serDeIn, serDeOut);
        this.setName("FlatMap-");
    }
    @Override
    public void run(){
        super.run();
    }
    @Override
    protected void processElement(ByteString in, OutputSender outputSender) {
        T data = (T) serdeIn.deserializeIn(in);
        output= UDFflatmap(data);
        for (T t: output){
            ByteString bs = serdeOut.serializeOut(t);
            outputSender.sendOutput(Tm.Msg.newBuilder().setType(Tm.Msg.MsgType.DATA).setData(bs));
        }
    }
    private List<T> UDFflatmap(T t){
        // some implementation
        return output;
    }
}