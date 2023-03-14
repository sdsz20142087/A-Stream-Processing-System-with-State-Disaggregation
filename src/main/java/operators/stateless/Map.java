package operators.stateless;

import com.google.protobuf.ByteString;
import exec.SerDe;
import operators.BaseOperator;
import pb.Op;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Map<T> extends BaseOperator implements Serializable {
    private SerDe<T> serde;
    public Map(Op.OperatorConfig config, SerDe<T> serde) {
        super(config);
        this.serde = serde;
    }

    @Override
    protected void processElement(ByteString in) {
        T data = serde.deserialize(in);
        T output= UDFmap(data);
        ByteString bs = serde.serialize(output);
        sendOutput(Op.Msg.newBuilder().setType(Op.Msg.MsgType.DATA).setData(bs).build());
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
