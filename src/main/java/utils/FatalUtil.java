package utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class FatalUtil {
    private static final Logger logger = LogManager.getLogger();

    public static void fatal(String context, Throwable t) {
        if(t==null){
            logger.fatal("Fatal error in " + context);
        } else {
            String stack = List.of(t.getStackTrace()).toString();
            String msg = "Fatal error in " + context + ": " + t.getMessage() + "\n stack: " + stack;
            logger.fatal(msg);
        }
        System.exit(1);
    }
}
