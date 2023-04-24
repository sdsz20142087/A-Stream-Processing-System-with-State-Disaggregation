package operators.stateful;

import operators.BaseOperator;
import operators.OutputSender;
import pb.Tm;
import utils.CPULoad;
import utils.SerDe;

public class StatefulCPUHeavyOperator<T> extends BaseOperator {

    private int loadMillis;

    public StatefulCPUHeavyOperator(SerDe<T> serdeIO, int loadMillis) {
        super(serdeIO, serdeIO);
        setOpName("StatefulCPUHeavyOperator");
        this.loadMillis = loadMillis;
    }

    @Override
    protected void processElement(Tm.Msg msg, OutputSender outputSender) {
        // 20ms computation
        CPULoad.loadCPU(loadMillis);
        Tm.Msg.Builder builder = Tm.Msg.newBuilder();
        builder.mergeFrom(msg);
        builder.setData(msg.getData());
        outputSender.sendOutput(builder.build());
    }
}
