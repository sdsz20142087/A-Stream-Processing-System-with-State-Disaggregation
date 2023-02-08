import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.MessageEvent;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams
 * https://stream.wikimedia.org/?doc#/streams
 *
 */
public class WikiTopServers {

    @Test
    public void receiveEvents() throws InterruptedException {
        WikipediaEventHandler eventHandler = new WikipediaEventHandler();
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));

        try (EventSource eventSource = builder.build()) {
            eventSource.start();

            TimeUnit.SECONDS.sleep(1000);
            System.out.println("Total: " + eventHandler.getNumEvents());
        }
    }

    private static class WikipediaEventHandler implements EventHandler {

        long numEvents = 0;
        HashMap<String, Integer> serverCounts = new HashMap<>();

        public long getNumEvents() {
            return this.numEvents;
        }

        @Override
        public void onOpen() throws Exception {

        }

        @Override
        public void onClosed() throws Exception {

        }

        @Override
        public void onMessage(String s, MessageEvent e) throws Exception {
            JsonNode parent = new ObjectMapper().readTree(e.getData());
            // Task 1: Filter out all events except edits
            String type = parent.path("type").asText();
            if (type.equals("edit")) {
                numEvents++;

                // Task 2: Project type and server
                String serverName = parent.path("server_name").asText();
                System.out.println("Event received with type *" + type + "* from server *" + serverName + "*");

                // Task 3: #events per server
                serverCounts.merge(serverName, 1, (i, j) -> i+j);
                System.out.println(serverCounts);
            }
        }

        @Override
        public void onComment(String s) throws Exception {

        }

        @Override
        public void onError(Throwable throwable) {

        }
    }
}
