package operators.stateful;

import operators.BaseOperator;
import operators.OutputSender;
import pb.Tm;
import stateapis.MapStateAccessor;
import utils.CPULoad;
import utils.SerDe;
import utils.WikiInfo;

public class ServerCountOperator extends BaseOperator {

    private final int emitEvery;
    // map < server-name, count >
    private transient MapStateAccessor<Integer> mapAccesor;
    private transient MapStateAccessor<String> junkAccessor;
    private final int cpuLoad;

    public ServerCountOperator(SerDe<WikiInfo> serdeIn, SerDe<String> serdeOut, int emitEvery, int cpuLoad) {
        super(serdeIn, serdeOut);
        setOpName("SvCountOperator");
        this.emitEvery = emitEvery;
        this.cpuLoad = cpuLoad;
    }

    @Override
    public void postInit() {
        mapAccesor = stateDescriptorProvider.getMapStateAccessor(this, "svNameMap");
        junkAccessor = stateDescriptorProvider.getMapStateAccessor(this, "junkMap");
    }

    private String makeJunkString(){
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<1000;i++){
            sb.append("junk"+i);
        }
        return sb.toString();
    }

    @Override
    protected void processElement(Tm.Msg msg, OutputSender outputSender) {

        WikiInfo wi = (WikiInfo) serdeIn.deserializeIn(msg.getData());
        logger.info("wi: {}", wi);
        Integer oldVal = mapAccesor.value().get(wi.server_name);

        int newVal = oldVal==null?1:oldVal + 1;
        long start = System.currentTimeMillis();
        mapAccesor.value().put(wi.server_name, newVal);
        for(int i=0;i<5;i++){
            junkAccessor.value().put("junk"+i, makeJunkString());
        }
        long end = System.currentTimeMillis();
        logger.info("Time to put junk: {}", end-start);
        CPULoad.loadCPU(cpuLoad);
        if(emitEvery==1 || newVal % emitEvery == 1){
            String outMsg = String.format("Count for server-name: <%s>: [%d]", wi.server_name, newVal);
            Tm.Msg.Builder builder = Tm.Msg.newBuilder();
            builder.mergeFrom(msg);
            builder.setData(serdeOut.serializeOut(outMsg));
            outputSender.sendOutput(builder.build());
        }
    }
}
