package operators.stateless;

import com.google.protobuf.ByteString;
import operators.OutputSender;
import utils.SerDe;
import operators.BaseOperator;
import pb.Tm;

import java.io.Serializable;

public class Map<Tin, Tout> extends BaseOperator implements Serializable {
    public Map(SerDe<Tin> serdeIn, SerDe<Tout> serdeOut) {
        super(serdeIn, serdeOut);
        this.setName("Map-");
    }

    @Override
    protected void processElement(ByteString in, OutputSender outputSender) {
        Tin data = (Tin) serdeIn.deserializeIn(in);
        Tout output= UDFmap(data);
        ByteString bs = serdeOut.serializeOut(output);
        outputSender.sendOutput(Tm.Msg.newBuilder().setType(Tm.Msg.MsgType.DATA).setData(bs));
    }


    @Override
    public void run(){
        super.run();
    }
    private Tout UDFmap(Tin t){
        // some implementation
        return null;
    }
}
