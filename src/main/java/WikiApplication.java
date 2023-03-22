import utils.ServerCount;
import utils.TimedMessageEvent;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *  A simple application that ingests wiki edit events
 *  and counts the number of events per server
 */
@Deprecated
public class WikiApplication {
    private static final String SERVER_URL = "https://stream.wikimedia.org/v2/stream/recentchange";
    private static final String FILE_INPUT = "data.txt";
    private static final int NUM_MAPPERS = 1;
    private static final int NUM_COUNTS = 1;

    public static void main(String[] args) {

        ConcurrentLinkedQueue<TimedMessageEvent>[] srcToMappers = new ConcurrentLinkedQueue[NUM_MAPPERS];
        ConcurrentLinkedQueue<ServerCount>[] mapperToCount = new ConcurrentLinkedQueue[NUM_COUNTS];
        ConcurrentLinkedQueue<String> countToSink = new ConcurrentLinkedQueue<>();

        // create src to mapper channels
        for (int i=0; i<NUM_MAPPERS; i++) {
            srcToMappers[i] = new ConcurrentLinkedQueue<>();
        }

        // create mappers to counters channels
        for (int i=0; i<NUM_COUNTS; i++) {
            mapperToCount[i] = new ConcurrentLinkedQueue<>();
        }

        //WikipediaSource src = new WikipediaSource(SERVER_URL, 30, srcToMappers, NUM_MAPPERS);
        WikipediaFileSource f_src = new WikipediaFileSource(FILE_INPUT, srcToMappers, NUM_MAPPERS);
        // create mappers
        JSonParserOperator[] mappers = new JSonParserOperator[NUM_MAPPERS];
        for (int i=0; i<NUM_MAPPERS; i++) {
            mappers[i] = new JSonParserOperator(srcToMappers[i], mapperToCount, NUM_COUNTS);
        };
        // create counts
        CountOperator[] counters = new CountOperator[NUM_COUNTS];
        for (int i=0; i<NUM_COUNTS; i++) {
            counters[i] = new CountOperator(mapperToCount[i], countToSink);
        };

        long start = System.nanoTime();
        // start counts
        for (int i=0; i<NUM_COUNTS; i++) {
            counters[i].start();
        }

        // start mappers
        for (int i=0; i<NUM_MAPPERS; i++) {
            mappers[i].start();
        }
        // start the source
        f_src.start();

        // Wait for all threads to die
        try {
            for (int i=0; i<NUM_COUNTS; i++) {
                counters[i].join();
            }
            for (int i=0; i<NUM_MAPPERS; i++) {
                mappers[i].join();
            }
            f_src.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Total: " + ((System.nanoTime() - start)/1000000.0));
    }
}
