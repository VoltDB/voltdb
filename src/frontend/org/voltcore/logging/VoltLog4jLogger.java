/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.logging;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.util.Enumeration;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.log4j.Appender;
import org.apache.log4j.DailyMaxRollingFileAppender;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.voltcore.logging.VoltLogger.CoreVoltLogger;
import org.voltcore.utils.ShutdownHooks;

/**
 * Implements the core logging functionality for VoltLogger specific to
 * Log4j.
 */
public class VoltLog4jLogger implements CoreVoltLogger {
    static {
        ResourceBundle rb = null;
        try {
            rb = ResourceBundle.getBundle("org/voltdb/utils/voltdb_logstrings");
        } catch (MissingResourceException e) {
            System.err.println("Couldn't find voltdb_logstrings resource bundle. Should be in voltdb_logstrings.properties.");
            e.printStackTrace(System.err);
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "Couldn't find voltdb_logstrings resource bundle. Should be in voltdb_logstrings.properties.",
                    true, e);
        }
        Logger.getRootLogger().setResourceBundle(rb);
    }


   /*
     * Encoding for various log settings that will fit in 3 bits
     */
    public static final long all = 0;
    public static final long trace = 1;
    public static final long debug = 2;
    public static final long info = 3;
    public static final long warn = 4;
    public static final long error = 5;
    public static final long fatal = 6;
    public static final long off = 7;

    /** Underlying Log4j logger */
    final Logger m_logger;

    /**
     * Convert the VoltLogger Level to the Log4j Level
     */
    static org.apache.log4j.Level getPriorityForLevel(Level level) {
        switch  (level) {
            case DEBUG:
                return org.apache.log4j.Level.DEBUG;
            case ERROR:
                return org.apache.log4j.Level.ERROR;
            case FATAL:
                return org.apache.log4j.Level.FATAL;
            case INFO:
                return org.apache.log4j.Level.INFO;
            case TRACE:
                return org.apache.log4j.Level.TRACE;
            case WARN:
                return org.apache.log4j.Level.WARN;
            default:
                return null;
        }
    }

    public VoltLog4jLogger(String className) {
        m_logger = Logger.getLogger(className);
    }

    @Override
    public boolean isEnabledFor(Level level) {
        return m_logger.isEnabledFor(getPriorityForLevel(level));
    }

    @Override
    public void log(Level level, Object message, Throwable t) {
        m_logger.log(getPriorityForLevel(level), message, t);
    }

    /**
     * Return a long containing the encoded status of every log level for the relevant loggers.
     * @return A long containing log levels for all the loggers available in the EE
     */
    @Override
    public long getLogLevels(VoltLogger[] voltloggers) {
        Logger[] loggers = new Logger[voltloggers.length];
        for (int i = 0; i < voltloggers.length; i++)
            loggers[i] = ((VoltLog4jLogger)voltloggers[i].m_logger).m_logger;

        long logLevels = 0;
        for (int ii = 0; ii < loggers.length; ii++) {
            final int level = loggers[ii].getEffectiveLevel().toInt();
            switch (level) {
            case org.apache.log4j.Level.TRACE_INT:
                logLevels |= trace << (ii * 3);
                break;
            case org.apache.log4j.Level.ALL_INT:
                logLevels |= all << (ii * 3);
                break;
            case org.apache.log4j.Level.DEBUG_INT:
                logLevels |= debug << (ii * 3);
                break;
            case org.apache.log4j.Level.ERROR_INT:
                logLevels |= error << (ii * 3);
                break;
            case org.apache.log4j.Level.FATAL_INT:
                logLevels |= fatal << (ii * 3);
                break;
            case org.apache.log4j.Level.INFO_INT:
                logLevels |= info << (ii * 3);
                break;
            case org.apache.log4j.Level.OFF_INT:
                logLevels |= off << (ii * 3);
                break;
            case org.apache.log4j.Level.WARN_INT:
                logLevels |= warn << (ii * 3);
                break;
                default:
                    throw new RuntimeException("Unhandled log level " + level);
            }
        }
        return logLevels;
    }

    @Override
    public void setLevel(Level level) {
        m_logger.setLevel(getPriorityForLevel(level));
    }

    /**
     * Static method to change the Log4j config globally.
     * @param xmlConfig The text of a Log4j config file.
     */
    public static void configure(String xmlConfig, File voltroot) {
        DOMConfigurator configurator = new DOMConfigurator();
        StringReader sr = new StringReader(xmlConfig.trim());
        configurator.doConfigure(sr, LogManager.getLoggerRepository());
        setFileLoggerRoot(voltroot);
    }

    /**
     * Static method to change the log directory root
     * @param logRootDH log directory root
     */
    public static void setFileLoggerRoot(File logRootDH) {

        // Make the LogManager shutdown hook the last thing to be done,
        // so that we'll get logging from any other shutdown behavior.
        ShutdownHooks.registerFinalShutdownAction(LogManager::shutdown);

        if (System.getProperty("log4j.configuration", "").toLowerCase().contains("/voltdb/tests/")) {
            return;
        }
        if (Boolean.parseBoolean(System.getProperty("DISABLE_LOG_RECONFIGURE", "false"))) {
            return;
        }
        checkArgument(logRootDH != null, "log root directory is null");

        File logDH = new File(logRootDH, "log");
        File napFH = new File(logDH, "volt.log");

        Logger rootLogger = LogManager.getRootLogger();

        // This code handles both types of FileAppenders if someone is using old log4j it will still
        // route path to voltdbroot/log/volt.log you cant have both appenders as having them is useless.
        @SuppressWarnings("unchecked")
        Enumeration<Appender> appen = rootLogger.getAllAppenders();
        boolean dailyMax = false;
        FileAppender fileAppendr = null;
        // Default pattern for both.
        String fileAppendrDatePattern = "'.'yyyy-MM-dd";
        // Default we use change this if default changes.
        int maxBackupIndex = 30;
        while (appen.hasMoreElements()) {
            Appender appndr = appen.nextElement();
            if (appndr instanceof DailyMaxRollingFileAppender) {
                dailyMax = true;
                DailyMaxRollingFileAppender oap = (DailyMaxRollingFileAppender) appndr;
                if (oap.getFile() != null) {
                    File logFH = new File(oap.getFile());
                    if (!logFH.isAbsolute()) {
                        fileAppendrDatePattern = oap.getDatePattern();
                        fileAppendr = oap;
                        maxBackupIndex = oap.getMaxBackupIndex();
                        break;
                    }
                }
            }
            // For older log4j
            if (appndr instanceof DailyRollingFileAppender) {
                DailyRollingFileAppender oap = (DailyRollingFileAppender) appndr;
                if (oap.getFile() != null) {
                    File logFH = new File(oap.getFile());
                    if (!logFH.isAbsolute()) {
                        fileAppendrDatePattern = oap.getDatePattern();
                        fileAppendr = oap;
                        break;
                    }
                }
            }
        }
        // We found a fileAppender that does not use absolute path or none
        if (fileAppendr == null) {
            return;
        }

        FileAppender nap;
        try {
            if (!logDH.exists() && !logDH.mkdirs()) {
                throw new IllegalArgumentException("failed to create directory " + logDH);
            }
            if (!logDH.isDirectory() || !logDH.canRead() || !logDH.canWrite() || !logDH.canExecute()) {
                throw new IllegalArgumentException("Cannot access " + logDH);
            }
            if (dailyMax) {
                DailyMaxRollingFileAppender dfa = new DailyMaxRollingFileAppender(fileAppendr.getLayout(), napFH.getPath(), fileAppendrDatePattern);
                dfa.setMaxBackupIndex(maxBackupIndex);
                nap = dfa;
            } else {
                nap = new DailyRollingFileAppender(fileAppendr.getLayout(), napFH.getPath(), fileAppendrDatePattern);
            }
            nap.setName(fileAppendr.getName());

            rootLogger.removeAppender(fileAppendr.getName());
            rootLogger.addAppender(nap);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to instantiate a FileAppender for file " + napFH, e);
        }

        if (fileAppendr.getFile() != null) {
            File oldFH = new File(fileAppendr.getFile());
            if (oldFH.exists() && oldFH.isFile() && oldFH.length() == 0L && oldFH.delete()) {
                File oldDH = oldFH.getParentFile();
                if (oldDH != null) {
                    String[] files = oldDH.list();
                    if (files != null && files.length == 0) {
                        oldDH.delete();
                    }
                }
            }
        }

        @SuppressWarnings("unchecked")
        Enumeration<Logger> e = LogManager.getCurrentLoggers();
        while (e.hasMoreElements()) {
            Logger lgr = e.nextElement();
            Appender apndr = lgr.getAppender(fileAppendr.getName());
            if (apndr != null) {
                lgr.removeAppender(fileAppendr.getName());
                lgr.addAppender(nap);
            }
        }
    }

    /**
     * For a log4j rolling file appender, get an estimate of the next
     * rollover time. We do this by looking at the implementation's
     * internals.
     *
     * This code was originally in RealVoltDB. I moved it here when
     * making it work with the DailyMaxRollingFileAppender as well as
     * with the DailyRollingFileAppender. I did not write this code.
     * It's not my fault.
     *
     * There are two really bad things about this: (1) the whole thing
     * about diddling around in the internals, and (2) it's an estimate
     * of when to check for rollover, and we treat it as when rollover
     * will occur.
     *
     * @return time (msec past epoch) of next check for rollover,
     * or -1 if anything goes wrong.
     */
    public static long getNextCheckTime() {

        // Find first (only) file appender of whatever kind
        FileAppender dailyAppender = null;
        Enumeration<?> appenders = Logger.getRootLogger().getAllAppenders();
        while (appenders.hasMoreElements()) {
            Appender appender = (Appender) appenders.nextElement();
            if (appender instanceof DailyMaxRollingFileAppender) {
                dailyAppender = (FileAppender)appender;
                break;
            }
            if (appender instanceof DailyRollingFileAppender) {
                dailyAppender = (FileAppender)appender;
                break;
            }
        }
        if (dailyAppender == null) {
            return -1;
        }

        // Grant ourselves access to the private field and then read its value
        long nextCheck;
        try {
            Field field = dailyAppender.getClass().getDeclaredField("nextCheck");
            field.setAccessible(true);
            nextCheck = field.getLong(dailyAppender);
        }
        catch (NoSuchFieldException | IllegalAccessException ex) {
            nextCheck = -1;
        }
        return nextCheck;
    }
}
