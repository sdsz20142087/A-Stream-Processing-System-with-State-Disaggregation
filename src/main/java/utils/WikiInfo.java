package utils;

import java.io.Serializable;

public class WikiInfo implements Serializable {
    public long id;
    public String server_name;

    public WikiInfo(long id, String server_name) {
        this.id = id;
        this.server_name = server_name;
    }
}