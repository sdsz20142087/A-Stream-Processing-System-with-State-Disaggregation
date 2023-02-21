package operators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class BaseOperator<IN, OUT> extends Thread{
    private LinkedBlockingQueue<IN> inputQueue;
    private LinkedBlockingQueue<OUT> outputQueue;
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

    public void addInput(IN input){
        inputQueue.add(input);
    }

    private void sendOutput(OUT output){
        outputQueue.add(output);
    }

    // emitting output is done in the processElement method
    protected abstract void processElement(IN input) throws Exception;

    public void run(){
        logger.info("Operator " + name + " started");
        // start a new thread that sends output to next operators
        Thread sender = new Thread(() -> {
            try {
                while (true) {
                    OUT output = outputQueue.take();

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        sender.start();

        try{
            while(true){
                IN input = inputQueue.take();
                processElement(input);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
