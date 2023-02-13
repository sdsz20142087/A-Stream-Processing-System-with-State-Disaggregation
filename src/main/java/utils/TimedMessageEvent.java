package utils;

import com.launchdarkly.eventsource.MessageEvent;

/**
 * A wrapper around MessageEvent that allows
 * recording when an event "entered" and "exited"
 * the system.
 */
public class TimedMessageEvent {
    private final MessageEvent e;
    private long enterTime;
    private long exitTime;

    public TimedMessageEvent(MessageEvent e, long t) {
        this.e = e;
        this.enterTime = t;
    }

    public TimedMessageEvent(String data, long t) {
        this.e = new MessageEvent(data);
        this.enterTime = t;
    }

    public void setEnterTime(long t) {
        this.enterTime = t;
    }

    public void setExitTime(long t) {
        this.exitTime = t;
    }

    public long getExitTime() {
        return exitTime;
    }

    public long getEnterTime() {
        return enterTime;
    }

    public String getData() {
        return this.e.getData();
    }

    @Override
    public String toString() {
        return getData() + ", " + getEnterTime();
    }
}
