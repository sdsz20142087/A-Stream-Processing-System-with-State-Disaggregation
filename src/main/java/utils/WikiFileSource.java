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

public class WikiFileSource implements ISource<String>, Serializable {


    private List<String> data = new ArrayList<>();
    private ListIterator<String> dataIter;
    private String path;

    public WikiFileSource(String path) {
        this.path = path;
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
        String d = dataIter.next();
        return d;
    }
}
