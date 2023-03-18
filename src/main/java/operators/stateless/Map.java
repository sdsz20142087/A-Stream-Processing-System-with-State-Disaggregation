package operators.stateless;

import com.google.protobuf.ByteString;
import exec.SerDe;
import operators.BaseOperator;
import pb.Tm;

import java.io.Serializable;

public class Map<T> extends BaseOperator implements Serializable {
    private SerDe<T> serde;
    public Map(Tm.OperatorConfig config, SerDe<T> serde) {
        super(config);
        this.serde = serde;
    }

    @Override
    protected void processElement(ByteString in) {
        T data = serde.deserialize(in);
        T output= UDFmap(data);
        ByteString bs = serde.serialize(output);
        sendOutput(Tm.Msg.newBuilder().setType(Tm.Msg.MsgType.DATA).setData(bs).build());
    }


    @Override
    public void run(){
        super.run();
    }
    private T UDFmap(T t){
        // some implementation
        return t;
    }
}
