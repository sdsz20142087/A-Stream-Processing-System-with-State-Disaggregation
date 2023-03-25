import java.util.concurrent.ConcurrentLinkedQueue;

@Deprecated
public abstract class SingleInputOperator<IN, OUT> extends Thread {
    /**
     * Apply transformation
     * @param element input event
     */

    public abstract void processElement(IN input);

}
