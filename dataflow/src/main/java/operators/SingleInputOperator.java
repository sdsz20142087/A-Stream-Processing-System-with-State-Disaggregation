package operators;

import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class SingleInputOperator<IN, OUT> extends Thread {
    //TODO output queue where it can push results
    /**
     * Apply transformation
     * @param element input event
     */

//    private ConcurrentLinkedQueue<IN> input;
//    private ConcurrentLinkedQueue<OUT> output;
//
//    public SingleInputOperator(ConcurrentLinkedQueue<IN> input, ConcurrentLinkedQueue<OUT> output){
//        this.input=input;
//        this.output=output;
//    }

    public abstract void processElement(IN input);

}
