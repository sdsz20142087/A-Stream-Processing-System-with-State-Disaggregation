package operators;

import com.google.protobuf.ByteString;
import kotlin.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingQueue;

import pb.Tm;
import stateapis.IKeyGetter;
import utils.FatalUtil;
import utils.SerDe;

public abstract class BaseOperator extends Thread implements Serializable, IKeyGetter {
    protected transient LinkedBlockingQueue<Tm.Msg> inputQueue;
    private transient LinkedBlockingQueue<Triple<String,ByteString,Integer>> outputQueue;
    protected transient Logger logger = LogManager.getLogger();
    private Tm.OperatorConfig config;
    private int bufferSize = 1000; // UDF buffer size, can change in runtime
    private static int paritionID = 0; // use for Round Robin

    protected StateDescriptorProvider stateDescriptorProvider;

    private String opName;

    protected SerDe serdeIn, serdeOut;

    private IKeySelector keySelector = null;

    private transient Object currentObj;

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
    }

    public final void init(Tm.OperatorConfig config, LinkedBlockingQueue<Tm.Msg> inputQueue,
                           LinkedBlockingQueue<Triple<String,ByteString,Integer>> outputQueue,
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

    public void setKeySelector(IKeySelector keySelector) {
    	this.keySelector = keySelector;
    }

    public boolean hasKeySelector() {
    	return keySelector != null;
    }

    public String getCurrentKey(){
        if(!this.hasKeySelector()){
            return null;
        }
        int keyInt = keySelector.getKey(currentObj);
        int desiredLength = 8;
        String hexString = Integer.toHexString(keyInt);
        hexString = String.format("%1$" + (desiredLength - 2) + "s", hexString).replace(' ', '0');
        hexString = "0x" + hexString;
        return hexString;
    }

    class BaseOutputSender implements OutputSender{
        private long ingestTime;
        public BaseOutputSender(long ingestTime){
            this.ingestTime = ingestTime;
        }

        public void sendOutput(Object o){
            int key = keySelector!=null?keySelector.getKey(o):-1;
            if(o instanceof ByteString){
                FatalUtil.fatal("Output is ByteString",null);
            }
            outputQueue.add(new Triple<>(config.getName(), serdeOut.serializeOut(o), -1));
        }
    }

    // emitting output is done in the processElement method
    protected abstract void processElement(ByteString in, OutputSender outputSender);
    @Override
    public void run() {
        if(config==null){
            FatalUtil.fatal("Operator not initialized",null);
        }
        // receive input from upstream operators
        try {
            while (true) {
                Tm.Msg input = inputQueue.take();
                if(input.getType()==Tm.Msg.MsgType.DATA){
                    Object o = serdeIn.deserializeIn(input.getData());
                    currentObj = o;
                }
                processElement(input.getData(), new BaseOutputSender(input.getIngestTime()));
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
