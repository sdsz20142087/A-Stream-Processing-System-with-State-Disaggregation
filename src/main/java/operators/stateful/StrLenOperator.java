package operators.stateful;

import operators.BaseOperator;
import operators.OutputSender;
import pb.Tm;
import stateapis.MapStateAccessor;
import stateapis.ValueStateAccessor;
import utils.SerDe;
import utils.WikiInfo;

public class StrLenOperator extends BaseOperator {

    private int emitEvery;
    // map < server-name, count >
    private transient MapStateAccessor<Integer> mapAccesor;

    public StrLenOperator(SerDe<WikiInfo> serdeIn, SerDe<String> serdeOut, int emitEvery) {
        super(serdeIn, serdeOut);
        this.emitEvery = emitEvery;
    }

    @Override
    public void postInit() {
        mapAccesor = stateDescriptorProvider.getMapStateAccessor(this, "svNameMap");
    }

    @Override
    protected void processElement(Tm.Msg msg, OutputSender outputSender) {

        WikiInfo wi = (WikiInfo) serdeIn.deserializeIn(msg.getData());
        int oldVal = mapAccesor.value().get(wi.server_name);
        int newVal = oldVal + 1;
        mapAccesor.value().put(wi.server_name, newVal);

        if(newVal % emitEvery == 0){
            String outMsg = String.format("Count for server-name: <%s>: [%d]", wi.server_name, newVal);
            Tm.Msg.Builder builder = Tm.Msg.newBuilder();
            builder.mergeFrom(msg);
            builder.setData(serdeOut.serializeOut(outMsg));
            outputSender.sendOutput(builder.build());
        }
    }
}
