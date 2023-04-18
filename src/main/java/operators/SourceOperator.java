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
                    startTimeStamp = System.currentTimeMillis();
                    firstElementFlag = false;
                    System.out.println("SOURCE startTimeStamp"+startTimeStamp);
                }
                T data = source.next();
                watermarkContent = data;
                ByteString bs = serdeIn.serializeOut(data);
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
                System.out.println("SOURCE OPERATOR setIngestTime:"+(System.currentTimeMillis() - startTimeStamp));
                Tm.Msg msg = Tm.Msg.newBuilder()
                        .setType(Tm.Msg.MsgType.DATA)
                        .setData(bs)
                        .setSenderOperatorName("DATA SOURCE")
                        .build();
                inputQueue.add(msg);
            }
        }).start();

//        try {
//            Thread.sleep(100);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }

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
                logger.info("Elapsed time: " + elapsedTime + " seconds");
                ByteString bs = serdeIn.serializeOut(watermarkContent);
                Tm.Msg msg = Tm.Msg.newBuilder()
                        .setType(Tm.Msg.MsgType.WATERMARK)
                        .setData(bs)
                        .setSenderOperatorName("DATA SOURCE")
                        .setIngestTime(Math.max(0, (System.currentTimeMillis()) - startTimeStamp - this.out_of_order_grace_period))
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
        Tm.Msg newMsg=msg.toBuilder().setIngestTime((System.currentTimeMillis() - startTimeStamp)).build();
        outputSender.sendOutput(newMsg);
    }

//    @Override
//    protected void processWatermark(ByteString in, OutputSender outputSender) {
//        T obj = (T) serdeIn.deserializeIn(in);
//        outputSender.sendOutput(obj);
//    }
}
