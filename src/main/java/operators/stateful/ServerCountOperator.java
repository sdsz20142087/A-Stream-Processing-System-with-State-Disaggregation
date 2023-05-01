package operators.stateful;

import operators.BaseOperator;
import operators.OutputSender;
import pb.Tm;
import stateapis.MapStateAccessor;
import utils.SerDe;
import utils.WikiInfo;

public class ServerCountOperator extends BaseOperator {

    private final int emitEvery;
    // map < server-name, count >
    private transient MapStateAccessor<Integer> mapAccesor;

    public ServerCountOperator(SerDe<WikiInfo> serdeIn, SerDe<String> serdeOut, int emitEvery) {
        super(serdeIn, serdeOut);
        setOpName("SvCountOperator");
        this.emitEvery = emitEvery;
    }

    @Override
    public void postInit() {
        mapAccesor = stateDescriptorProvider.getMapStateAccessor(this, "svNameMap");
    }

    @Override
    protected void processElement(Tm.Msg msg, OutputSender outputSender) {

        WikiInfo wi = (WikiInfo) serdeIn.deserializeIn(msg.getData());
        logger.info("wi: {}", wi);
        Integer oldVal = mapAccesor.value().get(wi.server_name);

        int newVal = oldVal==null?1:oldVal + 1;
        mapAccesor.value().put(wi.server_name, newVal);

        if(emitEvery==1 || newVal % emitEvery == 1){
            String outMsg = String.format("Count for server-name: <%s>: [%d]", wi.server_name, newVal);
            Tm.Msg.Builder builder = Tm.Msg.newBuilder();
            builder.mergeFrom(msg);
            builder.setData(serdeOut.serializeOut(outMsg));
            outputSender.sendOutput(builder.build());
        }
    }
}
