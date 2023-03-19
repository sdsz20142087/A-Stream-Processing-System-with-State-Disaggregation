package operators.stateful;

public class Window {
    private  long startTime;
    private  long endTime;

    public Window(long start, long end) {
        this.startTime = start;
        this.endTime = end;
    }
    public long getStart() {
        return startTime;
    }
    public long getEnd() {
        return endTime;
    }
    public boolean isWithinWindow(long timestamp) {
        return timestamp >= startTime && timestamp <= endTime;
    }
    public void setWindow(long start, long end) {
        this.startTime = start;
        this.endTime = end;
    }
}
