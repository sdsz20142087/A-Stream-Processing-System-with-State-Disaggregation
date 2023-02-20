package utils;

import org.apache.logging.log4j.LogManager;

import java.net.InetAddress;
import java.util.UUID;

public class NodeBase {

    private static String name = UUID.randomUUID().toString();

    public static String getName(){
        return getHost();
    }

    public static String getHost() {
        // get the ip of current node
        try{
            InetAddress addr = InetAddress.getLocalHost();
            return addr.getHostAddress();
        } catch (Exception e) {
            LogManager.getLogger().fatal("Failed to get host address" ,e);
        }
        return null;
    }
}
