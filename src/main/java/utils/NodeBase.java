package utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.util.UUID;

public class NodeBase {
    protected final Logger logger = LogManager.getLogger();

    protected static final String configPath = "config.json";
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
            FatalUtil.fatal("Failed to get host address" ,e);
        }
        return null;
    }
}
