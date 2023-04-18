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
    protected void processElement(Tm.Msg msg, OutputSender outputSender) {
        ByteString in = msg.getData();
        Tin data = (Tin) serdeIn.deserializeIn(in);
        Tout output= UDFmap(data);
        ByteString bs = serdeOut.serializeOut(output);
        Tm.Msg.Builder builder = Tm.Msg.newBuilder();
        builder.mergeFrom(msg);
        builder.setData(bs);
        outputSender.sendOutput(builder.build());
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
