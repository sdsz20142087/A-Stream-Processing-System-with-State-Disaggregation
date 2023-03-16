package operators.stateless;

import com.google.protobuf.ByteString;
import exec.SerDe;
import operators.BaseOperator;
import pb.Op;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FlatMap<T> extends BaseOperator implements Serializable{
    private SerDe<T> serde;
    List<T> output= new ArrayList<>();
    public FlatMap(Op.OperatorConfig config,SerDe<T> serde) {
        super(config);
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
            sendOutput(Op.Msg.newBuilder().setType(Op.Msg.MsgType.DATA).setData(bs).build());
        }
    }
    private List<T> UDFflatmap(T t){
        // some implementation
        return output;
    }
}