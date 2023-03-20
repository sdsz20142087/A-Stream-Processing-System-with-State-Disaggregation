package operators.stateless;

import com.google.protobuf.ByteString;
import exec.SerDe;
import operators.BaseOperator;
import pb.Tm;

import java.io.Serializable;

public class Map<T> extends BaseOperator implements Serializable {
    private SerDe<T> serde;
    public Map(SerDe<T> serde) {
        this.setName("Map-");
        this.serde = serde;
    }

    @Override
    protected void processElement(ByteString in) {
        T data = serde.deserialize(in);
        T output= UDFmap(data);
        ByteString bs = serde.serialize(output);
        sendOutput(Tm.Msg.newBuilder().setType(Tm.Msg.MsgType.DATA).setData(bs));
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
