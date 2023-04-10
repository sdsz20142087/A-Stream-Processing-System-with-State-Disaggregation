package utils;

import com.google.protobuf.ByteString;
import operators.ISource;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class WikiFileSource implements ISource<String>, Serializable {


    private List<String> data = new ArrayList<>();
    private ListIterator<String> dataIter;
    private String path;
    private BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    private long periodMillis;
    public WikiFileSource(String path,long periodMillis) {
        this.path = path;
        this.periodMillis= periodMillis;
    }

    @Override
    public void init() throws IOException {
        // read everything in advance because we are lazy
        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line = reader.readLine();
        while (line != null) {
            data.add(line);
            line = reader.readLine();
        }
        dataIter = data.listIterator();
        System.out.println("Wikifilesource: read " + data.size() + " lines from " + path);
    }

    @Override
    public boolean hasNext() {
        return dataIter.hasNext();
    }

    @Override
    public String next() {
        String d= queue.poll();
        return d;
    }
    public void startPeriodicWriting() {
        new Thread(() -> {
            try {
                String data = dataIter.next();
                // add ingest timestamp
                long ingestTime = System.currentTimeMillis();
                data = data + "," + ingestTime;
                queue.add(data);
                Thread.sleep(this.periodMillis);
            } catch (InterruptedException e) {
                throw new RuntimeException("Error adding data to queue", e);
            }
        }).start();
    }
}
