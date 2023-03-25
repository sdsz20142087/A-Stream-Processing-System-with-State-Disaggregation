import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import utils.ServerCount;
import utils.TimedMessageEvent;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *  A map operator that parses the json events and projects some fields
 */
@Deprecated
public class JSonParserOperator extends SingleInputOperator<TimedMessageEvent, ServerCount> {

    private final ConcurrentLinkedQueue<TimedMessageEvent> input;
    private final ConcurrentLinkedQueue<ServerCount>[] output;
    TimedMessageEvent next;
    private final int numDownStreamTasks;
    private int channelIndex;

    public JSonParserOperator(ConcurrentLinkedQueue<TimedMessageEvent> input, ConcurrentLinkedQueue<ServerCount>[] output,
                              int numDownStreamTasks) {
        this.input = input;
        this.output = output;
        this.numDownStreamTasks = numDownStreamTasks;
    }

    public void run() {
        while (input != null) {
            next = input.poll();
            if (next != null) {
                if (next.getData().equals("eof")) {
                    processElement(next);
                    break;
                }
                else {
                    // we have data to process
                    processElement(next);
                }
            }
        }
    }

    public void processElement(TimedMessageEvent next) {
        // termination
        if (next.getData().equals("eof")) {
               for (int i = 0; i < numDownStreamTasks; i++) {
                   output[i].add(new ServerCount("eof", 0));
               }
        }
            try {
            JsonNode parent = new ObjectMapper().readTree(next.getData());
            String type = parent.path("type").asText();
            String timestamp = parent.path("timestamp").asText();
            String user = parent.path("user").asText();
            String server_name = parent.path("server_name").asText();
            // select channel based on server name
            channelIndex = Math.abs(server_name.hashCode() % numDownStreamTasks);
            output[channelIndex].add(new ServerCount(server_name, 1));
        } catch (JsonProcessingException e) {
            //TODO: log exception
        }
    }

}
