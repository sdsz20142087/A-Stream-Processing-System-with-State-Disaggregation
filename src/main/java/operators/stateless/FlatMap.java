package operators.stateless;

import com.google.protobuf.ByteString;
import exec.SerDe;
import operators.BaseOperator;
import pb.Tm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FlatMap<T> extends BaseOperator implements Serializable{
    private SerDe<T> serde;
    List<T> output= new ArrayList<>();
    public FlatMap(SerDe<T> serde) {
        this.setName("FlatMap-");
        this.serde = serde;

    }
    @Override
    public void run(){
        super.run();
    }
    @Override
    protected void processElement(ByteString in) {
        T data = serde.deserialize(in);
        output= UDFflatmap(data);
        for (T t: output){
            ByteString bs = serde.serialize(t);
            sendOutput(Tm.Msg.newBuilder().setType(Tm.Msg.MsgType.DATA).setData(bs));
        }
    }
    private List<T> UDFflatmap(T t){
        // some implementation
        return output;
    }
}