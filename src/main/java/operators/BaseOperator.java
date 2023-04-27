package operators;

import com.google.protobuf.ByteString;
import kotlin.Triple;
import config.CPConfig;
import config.Config;
import kotlin.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import pb.Tm;
import stateapis.IKeyGetter;
import taskmanager.IStateMigration;
import utils.FatalUtil;
import utils.KeyUtil;
import utils.SerDe;

public abstract class BaseOperator extends Thread implements Serializable, IKeyGetter {
    protected transient LinkedBlockingQueue<Tm.Msg> inputQueue;
    private transient LinkedBlockingQueue<OutputMessage> outputQueue;
    protected transient Logger logger = LogManager.getLogger();
    private Tm.OperatorConfig config;
    private int bufferSize = 1000; // UDF buffer size, can change in runtime
    private static int paritionID = 0; // use for Round Robin
    protected transient CPConfig cpcfg;
    protected StateDescriptorProvider stateDescriptorProvider;
    private String opName;
    protected SerDe serdeIn, serdeOut;

    private IKeySelector keySelector = null;

    private transient Object currentObj;
    protected long startTimeStamp = (long) (System.currentTimeMillis() / 1000.0);
    protected boolean firstElementFlag = true;
    protected boolean sendWatermarkOrNot = true;
    //key: OperatorName, value: MaxWatermark. Use this to calculate min of the max watermark
    protected ConcurrentHashMap<String, Long> operatorMinWatermarkMap = new ConcurrentHashMap<>();
    protected double watermark_interval;
    protected long minOfMaxWatermark = Long.MAX_VALUE;
    //reconfigTimestamp: consistent reconfig timestamp, e.g. if reconfigTimeStamp = 35, before 35,
    // use original configuration, after 35, use updated configuration
    protected long reconfigTimestamp = Long.MAX_VALUE;
    private IStateMigration kvMigrator;
    public long getMinOfMaxWatermark() {
        return minOfMaxWatermark;
    }
    public String getOpName() {
    	return opName;
    }
    public void setOpName(String opName) {
    	this.opName = opName;
    }
    public long getReconfigTimestamp() {
        return reconfigTimestamp;
    }

    public void setReconfigTimestamp(long reconfigTimestamp) {
        this.reconfigTimestamp = reconfigTimestamp;
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
                           LinkedBlockingQueue<OutputMessage> outputQueue,
                           StateDescriptorProvider stateDescriptorProvider,
                           IStateMigration kvMigrator){
        this.config = config;
        this.opName = config.getName();
        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
        this.stateDescriptorProvider = stateDescriptorProvider;
        this.logger = LogManager.getLogger();
        this.kvMigrator = kvMigrator;
    }

    public void postInit(){}

    public Tm.OperatorConfig getConfig(){
        return this.config;
    }

    public void setConfig(Tm.OperatorConfig config) {
        logger.info("{} setConfig: " + config.getName(), this.getOpName());
        this.config = config;
    }

    public void setKeySelector(IKeySelector keySelector) {
    	this.keySelector = keySelector;
    }

    public boolean hasKeySelector() {
    	return keySelector != null;
    }

    public String getCurrentKey(){
        if(keySelector == null){
            return "";
        }
        return KeyUtil.objToKey(currentObj, keySelector);
    }

    class BaseOutputSender implements OutputSender{
        private long ingestTime;
        private final long extIngestTime;
        public BaseOutputSender(long ingestTime, long extIngestTime){
            this.ingestTime = ingestTime;
            this.extIngestTime = extIngestTime;
        }

        public void sendOutput(Tm.Msg msg){
            int key = -1;
            if(msg.getType() == Tm.Msg.MsgType.DATA && getConfig().getPartitionStrategy()== Tm.PartitionStrategy.HASH){
                Object o = serdeOut.deserializeIn(msg.getData());
                key = keySelector!=null?KeyUtil.hashStringToInt(keySelector.getUniqueKey(o)):-1;
            }
            Tm.Msg.Builder msgBuilder = Tm.Msg.newBuilder();
            msgBuilder
                    .setType(msg.getType())
                    .setIngestTime(ingestTime)
                    .setData(msg.getData())
                    .setReconfigMsg(msg.getReconfigMsg())
                    .setSenderOperatorName(config.getName());
            if(extIngestTime==0){
                msgBuilder.setExtIngestTime(System.currentTimeMillis());
            } else {
                msgBuilder.setExtIngestTime(extIngestTime);
            }
            outputQueue.add(new OutputMessage(config.getName(), msgBuilder, key));
        }
        public long getIngestTime() {
            return ingestTime;
        }

        public void setIngestTime(long ingestTime) {
            this.ingestTime = ingestTime;
        }
    }

    // emitting output is done in the processElement method
    protected abstract void processElement(Tm.Msg msg, OutputSender outputSender);


    protected long generateOutPutWatermark() {
        long outputWatermark = Long.MAX_VALUE;
        for (String watermarkKey : operatorMinWatermarkMap.keySet()) {
            outputWatermark = Math.min(outputWatermark, operatorMinWatermarkMap.get(watermarkKey));
        }
        return outputWatermark;
    }

    //TODO: we need to implement TIME_WINDOW operator (override processWatermark() function), which could apply watermark info, e.g. if time window
    //TODO: is 5, if it receives watermark = 5, it can process it and pass to downstream operator.
//<<<<<<< HEAD
//    protected void processWatermark(ByteString in, OutputSender outputSender) {
//        Object obj = serdeIn.deserializeIn(in);
//        outputSender.sendOutput(obj);
//        logger.info("(WATERMARK MESSAGE): " + outputSender.getIngestTime());
//=======
    protected void processWatermark(Tm.Msg msg, OutputSender outputSender) {
        long operatorMinWatermark = Math.max(operatorMinWatermarkMap.get(msg.getSenderOperatorName()), outputSender.getIngestTime());
        operatorMinWatermarkMap.put(msg.getSenderOperatorName(), operatorMinWatermark);
        long minOfMaxWatermark = generateOutPutWatermark();
        if (sendWatermarkOrNot) {
            outputSender.setIngestTime(minOfMaxWatermark);
            //logger.info("(WATERMARK MESSAGE SEND): " + minOfMaxWatermark);
            outputSender.sendOutput(msg);
            sendWatermarkOrNot = false;
        } else {
            //logger.info("(WATERMARK MESSAGE STASHED): " + outputSender.getIngestTime());
        }
    }

    protected void processDataFlow(Tm.Msg msg, OutputSender outputSender) {
        switch (msg.getType()) {
            case DATA:
                currentObj = serdeIn.deserializeIn(msg.getData());
                processElement(msg, outputSender);
                break;
            case CONTROL:
                //this.kvMigrator.handleStageMigration();
                Tm.OperatorConfig newConfig = msg.getReconfigMsg().getConfigMap().get(this.getOpName());
                logger.info("{} seeing newConfig: {}, {}",getOpName(), newConfig, msg.getReconfigMsg().getConfigMap())   ;
                if(newConfig != null){
                    this.setConfig(newConfig);
                    List<Tm.OperatorConfig> cfgs = new ArrayList<>();
                    for(Tm.OperatorConfig cfg: msg.getReconfigMsg().getConfigMap().values()){
                        if(this.getConfig().getLogicalStage()==cfg.getLogicalStage()){
                            cfgs.add(cfg);
                        }
                    }
                    this.kvMigrator.handleStageMigration(cfgs);
                }
                // pass the message downstream as broadcast
                outputSender.sendOutput(msg);
                break;
            case WATERMARK: processWatermark(msg, outputSender); break;
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
                if (!operatorMinWatermarkMap.containsKey(input.getSenderOperatorName())) {
                    operatorMinWatermarkMap.put(input.getSenderOperatorName(), 0L);
                }
                processDataFlow(input, new BaseOutputSender(input.getIngestTime(), input.getExtIngestTime()));
            }
        } catch (Exception e) {
            logger.error("Exception in sender thread",e);
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
