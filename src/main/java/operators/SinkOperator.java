package operators;
import com.google.protobuf.ByteString;
import pb.Tm;
import utils.FatalUtil;
import utils.SerDe;

import java.io.FileWriter;
import java.io.Serializable;

public class SinkOperator<T> extends BaseOperator implements Serializable {
    private Prometheus prometheus;
    private String fname = "sink"+System.currentTimeMillis()/1000+".txt";
    public SinkOperator(SerDe<T> in) {
        super(in, null);
        this.setName("SinkOperator");
        this.setOpName("SinkOperator");
    }

    @Override
    protected void processElement(Tm.Msg msg, OutputSender outputSender) {
        ByteString in = msg.getData();
        double latency = System.currentTimeMillis() - msg.getExtIngestTime();
        String printMsg = String.format("--> Sink Data: %s, Ingest time: %d, latency: %f ms",
                in.toStringUtf8(), msg.getExtIngestTime(), latency);
        //System.out.println(printMsg);
        logger.info(printMsg);
        prometheus.setIngestTimestampGauge(latency);
        try{
            FileWriter fw = new FileWriter(fname, true);
            fw.write(String.format("%d,%f\n", System.currentTimeMillis(), latency));
            fw.close();
        } catch (Exception e){
            e.printStackTrace();
            FatalUtil.fatal("Failed to write to file", e);
        }
    }

    @Override
    public void postInit() {
        super.postInit();
        prometheus = new Prometheus();

    }
    @Override
    protected void processWatermark(Tm.Msg msg, OutputSender outputSender) {
        //logger.info("-->WATERMARK: "+ msg.getIngestTime());
    }
}
