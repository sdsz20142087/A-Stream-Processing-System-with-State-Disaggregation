import utils.ServerCount;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *  A simple count operator that keeps the current count per server
 *  in a hashmap
 */
@Deprecated
public class CountOperator extends SingleInputOperator<ServerCount, String>  {

    private final ConcurrentLinkedQueue<ServerCount> input;
    private final ConcurrentLinkedQueue<String> output;
    ServerCount next;
    long num_Messages = 0;
    private HashMap<String, Long> serverCounts;

    public CountOperator(ConcurrentLinkedQueue<ServerCount> input, ConcurrentLinkedQueue<String> output) {
        this.input = input;
        this.output = output;
        this.serverCounts = new HashMap<>();
    }

    public void run() {
        while (input != null) {
            next = input.poll();
            if (next != null) {
                if (next.getName().equals("eof")) {
                    System.out.println("[" + this.getId() + "] messages: " + num_Messages);
                    break;
                }
                else {
                    // we have data to process
                    processElement(next);
                }
            }
        }
    }

    public void processElement(ServerCount next) {
        num_Messages++;
        serverCounts.merge(next.getName(), next.getCount(), (i, j) -> i+j);
        System.out.println(serverCounts);
    }

}
