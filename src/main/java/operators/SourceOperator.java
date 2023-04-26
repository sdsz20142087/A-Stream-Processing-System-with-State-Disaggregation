package operators;

import com.google.protobuf.ByteString;
import config.CPConfig;
import config.Config;
import utils.SerDe;
import pb.Tm;
import java.io.IOException;
import java.io.Serializable;


public class SourceOperator<T> extends BaseOperator implements Serializable {
    private ISource<T> source;
    private SerDe<T> serde;
    private int out_of_order_grace_period;
    private T watermarkContent;

    public SourceOperator(ISource<T> source, SerDe<T> serde) {
        super(serde, serde);
        this.setOpName("SourceOperator");
        this.source = source;
        this.serde = serde;
        this.out_of_order_grace_period = this.cpcfg.out_of_order_grace_period;
        // start a new thread to emit data and store them in the input queue
        // TODO: the design is NOT finalized yet! We are not yet sure how to handle
        // stream item serialization/de-serialization
    }

    @Override
    public void run(){

        new Thread(() -> {
            try {
                source.init();
            } catch (IOException e) {
                logger.fatal("source init failed: " + e.getMessage());
                System.exit(1);
            }
            while (true) {
                if (firstElementFlag) {
                    startTimeStamp = (long) (System.currentTimeMillis() / 1000.0);
                    firstElementFlag = false;
                }
                T data = source.next();
                watermarkContent = data;
                ByteString bs = serdeIn.serializeOut(data);
                Tm.Msg msg = Tm.Msg.newBuilder()
                        .setType(Tm.Msg.MsgType.DATA)
                        .setData(bs)
                        .setSenderOperatorName("DATA SOURCE")
                        .setIngestTime((long) (System.currentTimeMillis() / 1000.0) - startTimeStamp)
                        .build();
                inputQueue.add(msg);
            }
        }).start();

        new Thread(() -> {
            while (true) {
                long startTime = System.currentTimeMillis();
                try {
                    Thread.sleep((long) (this.watermark_interval * 1000));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                long endTime = System.currentTimeMillis();
                double elapsedTime = (endTime - startTime) / 1000.0;
                //logger.info("Elapsed time: " + elapsedTime + " seconds");
                ByteString bs = serdeIn.serializeOut(watermarkContent);
                Tm.Msg msg = Tm.Msg.newBuilder()
                        .setType(Tm.Msg.MsgType.WATERMARK)
                        .setData(bs)
                        .setSenderOperatorName("DATA SOURCE")
                        .setIngestTime(Math.max(0, (long) (System.currentTimeMillis() / 1000.0) - startTimeStamp - this.out_of_order_grace_period))
                        .build();
                inputQueue.add(msg);
            }
        }).start();
        super.run();
    }

    @Override
    // simply move whatever we have in the input queue to the output queue
    protected void processElement(Tm.Msg msg, OutputSender outputSender) {
//        T obj = (T) serdeIn.deserializeIn(msg.getData());

        outputSender.sendOutput(msg);
    }

//    @Override
//    protected void processWatermark(ByteString in, OutputSender outputSender) {
//        T obj = (T) serdeIn.deserializeIn(in);
//        outputSender.sendOutput(obj);
//    }
}
