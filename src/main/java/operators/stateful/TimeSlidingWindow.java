package operators.stateful;

import com.google.protobuf.ByteString;
import exec.SerDe;
import operators.BaseOperator;
import pb.Tm;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

public class TimeSlidingWindow<IN,OUT> extends BaseOperator implements Serializable {
    private SerDe<IN> serde;
    private SerDe<OUT> serdeOut;
    private Window currentWindow;
    private long windowSize;
    private long slideStep;
    private ArrayList<IN> windowData;
    public TimeSlidingWindow(Tm.OperatorConfig config, SerDe<IN> serde, SerDe<OUT> serdeOut, long windowSize, long slideStep) {
        super(config);
        this.serde = serde;
        this.windowSize = windowSize;
        this.slideStep = slideStep;
        this.serdeOut = serdeOut;
        this.windowData = new ArrayList<>();
    }

    @Override
    protected void processElement(ByteString in) {
        IN data = serde.deserialize(in);
        long timestamp = getDataTimestamp(data);
        if(currentWindow == null){
            currentWindow = new Window(timestamp,timestamp+windowSize);
        }
        if(currentWindow.isWithinWindow(timestamp)){
            windowData.add(data);
            if(trigger()){
                OUT result = UDF(data);
                ByteString output=serdeOut.serialize(result);
                sendOutput(Tm.Msg.newBuilder().setType(Tm.Msg.MsgType.DATA).setData(output).build());
            }
        }else{
            moveWindow();
            removeOldData();
            windowData.add(data);
        }
    }
    // A user defeined trigger for when we can use the data in the window
    // e.g. count the number of data in the window when the number of data reaches a threshold
    public boolean trigger(){
        // need to be implemented by the user
        return false;
    }
    public OUT UDF(IN data){
        // need to be implemented by the user
        return null;
    }
    public Window getCurrentWindow(){
        return this.currentWindow;
    }
    public void moveWindow(){
        long newStart = currentWindow.getStart()+slideStep;
        long newEnd = newStart+windowSize;
        currentWindow.setWindow(newStart,newEnd);
    }
    public void removeOldData(){
        Iterator<IN> iterator = windowData.iterator();
        while (iterator.hasNext()) {
            IN item = iterator.next();
            if (!currentWindow.isWithinWindow(getDataTimestamp(item))) {
                iterator.remove();
            }
        }
    }
    public long getDataTimestamp(IN data){
        // TODO: need to be implemented by the user based on the data type
        return 0;
    }
    @Override
    public void run(){
        super.run();
    }
}
