package operators;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import kotlin.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import pb.Tm;

public abstract class BaseOperator extends Thread implements Serializable {
    protected transient LinkedBlockingQueue<Tm.Msg> inputQueue;
    private transient LinkedBlockingQueue<Pair<String,Tm.Msg>> outputQueue;
    protected transient Logger logger = LogManager.getLogger();
    private Tm.OperatorConfig config;
    private int bufferSize = 1000; // UDF buffer size, can change in runtime
    private static int paritionID = 0; // use for Round Robin

    // There must not be moving parts (e.g. listening to ports, starting new threads)
    // in the constructor because we'll be sending this object over grpc.
    public BaseOperator() {
    }

    public final void init(Tm.OperatorConfig config, LinkedBlockingQueue<Tm.Msg> inputQueue,
                           LinkedBlockingQueue<Pair<String,Tm.Msg>> outputQueue){
        this.config = config;
        this.setName(config.getName());
        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
    }

    public Tm.OperatorConfig getConfig(){
        return this.config;
    }

    protected void sendOutput(Tm.Msg output) {
        outputQueue.add(new Pair<>(config.getName(), output));
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

    // emitting output is done in the processElement method
    protected abstract void processElement(ByteString in);
    @Override
    public void run() {
        if(config==null){
            logger.fatal("Operator not initialized");
            System.exit(1);
        }
        // receive input from upstream operators
        try {
            while (true) {
                Tm.Msg input = inputQueue.take();
                processElement(input.getData());
            }
        } catch (Exception e) {
            logger.fatal("Exception in sender thread: " + e.getMessage());
            System.exit(1);
        }
        logger.info("Operator " + config.getName() + " started");
    }
    public boolean checkBuffer(){
        return inputQueue.size() > bufferSize;
    }

}
