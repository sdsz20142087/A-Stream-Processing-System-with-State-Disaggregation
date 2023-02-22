package operators;

import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pb.Op;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class BaseOperator extends Thread{
    private LinkedBlockingQueue<Op.Msg> inputQueue;
    private LinkedBlockingQueue<Op.Msg> outputQueue;
    private List<String> nextOpAddrs;
    private int keyByIndex;
    private String name;
    private Logger logger = LogManager.getLogger();

    public BaseOperator(String name, List<String> nextOpAddrs, int keyByIndex){
        this.inputQueue = new LinkedBlockingQueue<>();
        this.outputQueue = new LinkedBlockingQueue<>();
        this.nextOpAddrs = nextOpAddrs;
        this.keyByIndex = keyByIndex;
        this.name = name;
    }
    // external interface for adding input by TM
    public void addInput(Op.Msg input){
        inputQueue.add(input);
    }

    private void sendOutput(Op.Msg output){
        outputQueue.add(output);
    }

    private void handleMsg(Op.Msg input){
        switch (input.getType()){
            case DATA:
                processElement(input.getData());
                break;
            case CONTROL:
                // do something about the control msg
                logger.info("got control msg: " + input);
                // send it downstream
                sendOutput(input);
                break;
        }
    }

    // emitting output is done in the processElement method
    protected abstract void processElement(ByteString in);

    public void run(){
        logger.info("Operator " + name + " started");
        // start a new thread that sends output to next operators
        Thread sender = new Thread(() -> {
            try {
                while (true) {
                    Op.Msg output = outputQueue.take();

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        sender.start();

        try{
            while(true){
                Op.Msg input = inputQueue.take();
                handleMsg(input);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
