package operators;
import com.google.protobuf.ByteString;
import pb.Tm;
import java.io.Serializable;

public class SinkOperator extends BaseOperator implements Serializable {
    private Prometheus prometheus;
    public SinkOperator() {
        super(null, null);
        this.setName("SinkOperator");
        this.setOpName("SinkOperator");
    }

    @Override
    protected void processElement(Tm.Msg msg, OutputSender outputSender) {
        ByteString in = msg.getData();
//        System.out.println("-->DATA "+in.toStringUtf8());
//        System.out.println("Ingest time: "+msg.getIngestTime());
//        System.out.println("SINK OPERATOR TIME STAMP"+(System.currentTimeMillis() - startTimeStamp));
//        System.out.println("Gauge set:"+((System.currentTimeMillis() - startTimeStamp)-msg.getIngestTime()));
        prometheus.setIngestTimestampGauge((System.currentTimeMillis() - startTimeStamp)-msg.getIngestTime());
    }

    @Override
    public void postInit() {
        super.postInit();
        prometheus = new Prometheus();
    }
    @Override
    protected void processWatermark(Tm.Msg msg, OutputSender outputSender) {
        ByteString in = msg.getData();
        System.out.println("-->WATERMARK: "+ msg.getIngestTime());
    }
}
