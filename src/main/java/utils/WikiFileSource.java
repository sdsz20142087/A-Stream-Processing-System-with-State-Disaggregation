package utils;

import com.google.protobuf.ByteString;
import operators.ISource;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class WikiFileSource implements ISource {


    private List<String> data = new ArrayList<>();
    private ListIterator<String> dataIter;

    public WikiFileSource(String path) {
        // read everything in advance because we are lazy
        try{
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();
            while (line != null) {
                data.add(line);
                line = reader.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        dataIter = data.listIterator();
    }

    @Override
    public boolean hasNext() {
        return dataIter.hasNext();
    }

    @Override
    public ByteString next() {
        String d = dataIter.next();
        return ByteString.copyFromUtf8(d);
    }
}
