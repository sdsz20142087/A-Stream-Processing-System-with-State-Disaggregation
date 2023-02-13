package utils;

/**
 * A helper class to keep a server name and its current count
 */
public class ServerCount {
    private final String serverName;
    private long count;

    public ServerCount(String serverName, long cnt) {
        this.serverName = serverName;
        count = cnt;
    }

    public void increaseCount() {
        count++;
    }

    public long getCount() {
        return count;
    }

    public String getName() {
        return serverName;
    }
}
