package org.voltdb.utils;

import java.util.Collections;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;

public class LoggerHelper {
    /*
     * Surround the log with stars
     */
    public static void PrintGoodLookingLog(VoltLogger vLogger, String msg, Level level)
    {
        if (vLogger == null || msg == null || level == Level.OFF) { return; }

        // The border of the ascii-art-surrounded message
        // **********
        // * msg... *
        // **********
        String stars = String.join("", Collections.nCopies(msg.length() + 4, "*"));
        String new_msg = "* " + msg + " *";

        switch (level) {
            case DEBUG:
                vLogger.debug(stars);
                vLogger.debug(new_msg);
                vLogger.debug(stars);
                break;
            case WARN:
                vLogger.warn(stars);
                vLogger.warn(new_msg);
                vLogger.warn(stars);
                break;
            case ERROR:
                vLogger.error(stars);
                vLogger.error(new_msg);
                vLogger.error(stars);
                break;
            case FATAL:
                vLogger.fatal(stars);
                vLogger.fatal(new_msg);
                vLogger.fatal(stars);
                break;
            case INFO:
                vLogger.info(stars);
                vLogger.info(new_msg);
                vLogger.info(stars);
                break;
            case TRACE:
                vLogger.trace(stars);
                vLogger.trace(new_msg);
                vLogger.trace(stars);
                break;
            default:
                break;
        }
    }
}
