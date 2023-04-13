package utils;

import operators.ISource;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/*
    Can read file from local disk
 */
public class ClusterDataFileSource extends Thread implements ISource<String>, Serializable {
    private final List<String> FILE_PATH_List;
    private List<String> data = new ArrayList<>();
    private ListIterator<String> dataIter;
    private Iterator<String> filePathIterator;
    private BufferedReader reader;

    public ClusterDataFileSource(List<String>  filePathList ) {
        this.FILE_PATH_List = filePathList;
        this.filePathIterator = this.FILE_PATH_List.iterator();
    }
    private void read() throws IOException{
        while (reader != null) {
            String line = reader.readLine();
            while (line != null) {
                data.add(line);
                line = reader.readLine();
            }
            reader.close();
            openNextFile();
        }
    }
    private void openNextFile() throws IOException {
        if (filePathIterator.hasNext()) {
            String filePath = filePathIterator.next();
            reader = new BufferedReader(new FileReader(filePath));
        } else {
            reader = null;
        }
    }
    @Override
    public void init() throws IOException {
        openNextFile();
        read();
    }

    @Override
    public String next() {
        if (!data.isEmpty()) {
            return data.remove(0);
        }
        return null;
    }

    public static void main(String[] args) {
        List<String> filePathList = new ArrayList<>();
        filePathList.add("/Users/wzr/Downloads/data/task_events/part-00000-of-00500.csv");
        filePathList.add("/Users/wzr/Downloads/data/task_events/part-00001-of-00500.csv");
        // Add more file paths if needed

        ClusterDataFileSource clusterDataFileSource = new ClusterDataFileSource(filePathList);
        try {
            clusterDataFileSource.init();
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (int i = 0; i < 10; i++) {
            System.out.println(clusterDataFileSource.next());
        }
    }
}
