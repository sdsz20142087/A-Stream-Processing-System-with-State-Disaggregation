import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import utils.TimedMessageEvent;

import java.net.URI;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Deprecated
public class WikipediaSource extends Thread {
    private final String URL;
    private final long timeout;
    private final ConcurrentLinkedQueue<TimedMessageEvent>[] output;
    private final int numDownStreamTasks;
    private int channelIndex;

    public WikipediaSource(String URL, long timeout, ConcurrentLinkedQueue<TimedMessageEvent>[] out, int numDownStreamTasks) {
        this.URL = URL;
        this.timeout = timeout;
        this.output = out;
        this.numDownStreamTasks = numDownStreamTasks;
    }

    public void run() {
        startReceiving();
    }

    private void startReceiving() {
        WikipediaEventHandler handler = new WikipediaEventHandler();
        EventSource.Builder builder = new EventSource.Builder(handler, URI.create(URL));
        try (EventSource eventSource = builder.build()) {
            eventSource.start();
            TimeUnit.SECONDS.sleep(timeout);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processMessage(MessageEvent msg) {
        TimedMessageEvent tEvent = new TimedMessageEvent(msg, System.nanoTime());
        output[channelIndex].add(tEvent);
    }

    private void cleanUp() {
        for (int i=0; i<numDownStreamTasks; i++)
            output[i].add(new TimedMessageEvent("eof", System.nanoTime()));
    }

    private class WikipediaEventHandler implements EventHandler {

        long numEvents = 0;
        long numErrors = 0;

        @Override
        public void onOpen() throws Exception {
            channelIndex = 0;

        }

        @Override
        public void onClosed() throws Exception {
            System.out.println("Messages consumed: " + numEvents);
            System.out.println("Errors: " + numErrors);
            cleanUp();
        }

        @Override
        public void onMessage(String s, MessageEvent e) throws Exception {
            channelIndex = (channelIndex+1) % numDownStreamTasks;
            processMessage(e);
            numEvents++;
        }

        @Override
        public void onComment(String s) throws Exception {

        }

        @Override
        public void onError(Throwable throwable) {
            System.out.println("An error occurred: " + throwable.getMessage());
            numErrors++;
        }
    }

}
