package operators.stateful;

import com.google.protobuf.ByteString;
import operators.OutputSender;
import utils.SerDe;
import operators.BaseOperator;
import pb.Tm;
import stateapis.IDataflowMap;
import stateapis.MapStateAccessor;
import stateapis.ValueStateAccessor;

import java.io.Serializable;
import java.util.*;

public class TimeSlidingWindow<IN,OUT> extends BaseOperator implements Serializable {
    private Window currentWindow;
    private long windowSize;
    private long slideStep;
    private ArrayList<IN> windowData;

    private MapStateAccessor someMapStateAccessor;
    private ValueStateAccessor<Integer> intStateAccessor;
    public TimeSlidingWindow(SerDe<IN> serde, SerDe<OUT> serdeOut, long windowSize, long slideStep) {
        super(serde, serdeOut);
        this.windowSize = windowSize;
        this.slideStep = slideStep;
        this.windowData = new ArrayList<>();

        //
        someMapStateAccessor = stateDescriptorProvider.getMapStateAccessor(this, "some-map-state");
        intStateAccessor = stateDescriptorProvider.getValueStateAccessor(this, "some-int-state",0);
    }

    @Override
    protected void processElement(Tm.Msg msg, OutputSender outputSender) {
        ByteString in = msg.getData();

        IN data = (IN) this.serdeIn.deserializeIn(in);
        long timestamp = getDataTimestamp(data);
        if(currentWindow == null){
            currentWindow = new Window(timestamp,timestamp+windowSize);
        }
        if(currentWindow.isWithinWindow(timestamp)){
            windowData.add(data);
            if(trigger()){
                OUT result = UDF(data);
                ByteString output = serdeOut.serializeOut(result);
                Tm.Msg.Builder builder = Tm.Msg.newBuilder();
                builder.mergeFrom(msg);
                builder.setData(output);
                outputSender.sendOutput(builder.build());
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
