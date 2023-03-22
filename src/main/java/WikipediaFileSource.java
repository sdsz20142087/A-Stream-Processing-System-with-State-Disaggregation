import com.launchdarkly.eventsource.MessageEvent;
import utils.TimedMessageEvent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 */
@Deprecated
public class WikipediaFileSource extends Thread {
    private final String FILE_PATH;
    private final ConcurrentLinkedQueue<TimedMessageEvent>[] output;
    private final int numDownStreamTasks;
    private int channelIndex;
    private long numEvents;
    private long numErrors;

    public WikipediaFileSource(String filePath, ConcurrentLinkedQueue<TimedMessageEvent>[] out, int numDownStreamTasks) {
        this.FILE_PATH = filePath;
        this.output = out;
        this.numDownStreamTasks = numDownStreamTasks;
    }

    public void run() {
        startReceiving();
    }

    private void startReceiving() {
        channelIndex = 0;
        numEvents = 0;
        numErrors = 0;

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(FILE_PATH));
            String line = reader.readLine();
            while (line != null) {
                //System.out.println(line);
                MessageEvent msg = new MessageEvent(line);
                channelIndex = (channelIndex+1) % numDownStreamTasks;
                processMessage(msg);
                numEvents++;
                // read next line
                line = reader.readLine();
            }
            reader.close();
            cleanUp();
        } catch (IOException e) {
            numErrors++;
            e.printStackTrace();
        }


    }

    private void processMessage(MessageEvent msg) {
        TimedMessageEvent tEvent = new TimedMessageEvent(msg, System.nanoTime());
        output[channelIndex].add(tEvent);
    }

    private void cleanUp() {
        System.out.println("Messages consumed: " + numEvents);
        System.out.println("Errors: " + numErrors);
        for (int i=0; i<numDownStreamTasks; i++)
            output[i].add(new TimedMessageEvent("eof", System.nanoTime()));
    }

}
