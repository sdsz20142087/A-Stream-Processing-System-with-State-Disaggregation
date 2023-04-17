package operators;

import com.google.protobuf.ByteString;
import kotlin.Triple;
import config.CPConfig;
import config.Config;
import kotlin.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import pb.Tm;
import utils.DefaultKeySelector;
import utils.FatalUtil;
import utils.SerDe;

public abstract class BaseOperator extends Thread implements Serializable {
    protected transient LinkedBlockingQueue<Tm.Msg> inputQueue;
    private transient LinkedBlockingQueue<Triple<String, ByteString, Pair<Integer, Tm.Msg.MsgType>>> outputQueue;
    protected transient Logger logger = LogManager.getLogger();
    private Tm.OperatorConfig config;
    private int bufferSize = 1000; // UDF buffer size, can change in runtime
    private static int paritionID = 0; // use for Round Robin
    protected transient CPConfig cpcfg;
    protected StateDescriptorProvider stateDescriptorProvider;
    private String opName;
    protected SerDe serdeIn, serdeOut;

    private IKeySelector keySelector = new DefaultKeySelector();
    protected Tm.Msg currentInputMsg;
    protected long startTimeStamp = (long) (System.currentTimeMillis() / 1000.0);
    protected boolean firstElementFlag = true;
    protected boolean sendWatermarkOrNot = true;
    //key: OperatorName, value: MaxWatermark. Use this to calculate min of the max watermark
    protected ConcurrentHashMap<String, Long> operatorMinWatermarkMap = new ConcurrentHashMap<>();
    protected double watermark_interval;

    public String getOpName() {
    	return opName;
    }
    public void setOpName(String opName) {
    	this.opName = opName;
    }
    // There must not be moving parts (e.g. listening to ports, starting new threads)
    // in the constructor because we'll be sending this object over grpc.
    public BaseOperator(SerDe serdeIn, SerDe serdeOut) {
        this.serdeIn = serdeIn;
        this.serdeOut = serdeOut;
        this.cpcfg = Config.getInstance().controlPlane;
        this.watermark_interval = this.cpcfg.watermark_interval;
    }

    public final void init(Tm.OperatorConfig config, LinkedBlockingQueue<Tm.Msg> inputQueue,
                           LinkedBlockingQueue<Triple<String, ByteString, Pair<Integer, Tm.Msg.MsgType>>> outputQueue,
                           StateDescriptorProvider stateDescriptorProvider){
        this.config = config;
        this.opName = config.getName();
        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
        this.stateDescriptorProvider = stateDescriptorProvider;
        this.logger = LogManager.getLogger();
    }

    public void postInit(){}

    public Tm.OperatorConfig getConfig(){
        return this.config;
    }

    public void setConfig(Tm.OperatorConfig config) {
        this.config = config;
    }

    // !! No control message reaches the operator, only data messages
//    private void handleMsg(Tm.Msg input) {
//        switch (input.getType()) {
//            case DATA:
//                processElement(input.getData());
//                break;
//            case CONTROL:
//                // do something about the control msg
//                logger.info("got control msg: " + input);
//                // send it downstream
//                sendOutput(input);
//                break;
//        }
//    }

    class BaseOutputSender implements OutputSender{
        private long ingestTime;
        public BaseOutputSender(long ingestTime){
            this.ingestTime = ingestTime;
        }

        public void sendOutput(Object o){
            int key = keySelector.getKey(o);
            if(o instanceof ByteString){
                FatalUtil.fatal("Output is ByteString",null);
            }
            outputQueue.add(new Triple<>(config.getName(), serdeOut.serializeOut(o), new Pair<>(key, BaseOperator.this.currentInputMsg.getType())));
        }
        public long getIngestTime() {
            return ingestTime;
        }
<<<<<<< HEAD
=======

        public void setIngestTime(long ingestTime) {
            this.ingestTime = ingestTime;
        }

        public void sendOutput(Tm.Msg.Builder output){
            output.setIngestTime(ingestTime);
            output.setOperatorName(config.getName());
            outputQueue.add(new Pair<>(config.getName(), output));
        }
>>>>>>> 6253075... FINISHED propagate min of the max watermarks to downstream workers
    }

    // emitting output is done in the processElement method
    protected abstract void processElement(ByteString in, OutputSender outputSender);


    protected long generateOutPutWatermark() {
        long outputWatermark = Long.MAX_VALUE;
        for (String watermarkKey : operatorMinWatermarkMap.keySet()) {
            outputWatermark = Math.min(outputWatermark, operatorMinWatermarkMap.get(watermarkKey));
        }
        return outputWatermark;
    }

    //TODO: we need to implement TIME_WINDOW operator (override processWatermark() function), which could apply watermark info, e.g. if time window
    //TODO: is 5, if it receives watermark = 5, it can process it and pass to downstream operator.
<<<<<<< HEAD
    protected void processWatermark(ByteString in, OutputSender outputSender) {
        Object obj = serdeIn.deserializeIn(in);
        outputSender.sendOutput(obj);
        logger.info("(WATERMARK MESSAGE): " + outputSender.getIngestTime());
=======
    protected void processWatermark(ByteString in, String operatorName, OutputSender outputSender) {
        long operatorMinWatermark = Math.max(operatorMinWatermarkMap.get(operatorName), outputSender.getIngestTime());
        operatorMinWatermarkMap.put(operatorName, operatorMinWatermark);
        if (sendWatermarkOrNot) {
            long minOfMaxWatermark = generateOutPutWatermark();
            outputSender.setIngestTime(minOfMaxWatermark);
            logger.info("(WATERMARK MESSAGE SEND): " + minOfMaxWatermark);
            outputSender.sendOutput(Tm.Msg.newBuilder()
                    .setType(Tm.Msg.MsgType.WATERMARK)
                    .setData(in)
            );
            sendWatermarkOrNot = false;
        } else {
            logger.info("(WATERMARK MESSAGE STASHED): " + outputSender.getIngestTime());
        }
>>>>>>> 6253075... FINISHED propagate min of the max watermarks to downstream workers
    }

    protected void processDataFlow(ByteString in, Tm.Msg.MsgType type, String operatorName, OutputSender outputSender) {
        switch (type) {
            case DATA: processElement(in, outputSender); break;
            case CONTROL: break;
            case WATERMARK: processWatermark(in, operatorName, outputSender); break;
        }
    }
    @Override
    public void run() {
        if(config==null){
            FatalUtil.fatal("Operator not initialized",null);
        }

        //each operator sends watermark according to watermark interval. Although watermark interval is used by source before,
        //an operator will have multi downstream operator, which will generate more watermark at a specific time.
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep((long) (this.watermark_interval * 1000));
                    sendWatermarkOrNot = true;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();

        // receive input from upstream operators
        try {
            while (true) {
                Tm.Msg input = inputQueue.take();
                this.currentInputMsg = input;
                if (!operatorMinWatermarkMap.containsKey(input.getOperatorName())) {
                    operatorMinWatermarkMap.put(input.getOperatorName(), 0L);
                }
//                logger.info("--------");
//                for (String checkMap : operatorMinWatermarkMap.keySet()) {
//                    logger.info(checkMap + ": " + operatorMinWatermarkMap.get(checkMap));
//                }
//                logger.info("--------");
//                processElement(input.getData(), new BaseOutputSender(input.getIngestTime()));
                processDataFlow(input.getData(), input.getType(), input.getOperatorName(), new BaseOutputSender(input.getIngestTime()));
            }
        } catch (Exception e) {
            FatalUtil.fatal(getOpName()+": Exception in sender thread",e);
        }
        logger.info("Operator " + config.getName() + " started");
    }
    public boolean checkBuffer(){
        return inputQueue.size() > bufferSize;
    }

    public int getInputQueueLength() {
        return inputQueue.size();
    }

    public int getOutputQueueLength() {
        return outputQueue.size();
    }

}
