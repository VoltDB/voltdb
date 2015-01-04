/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.io.StringReader;
import java.io.StringWriter;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

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
            System.err.println("Couldn't find voltdb_logstrings resource bundle. Should be in voldb_logstrings.properties.");
            e.printStackTrace(System.err);
            org.voltdb.VoltDB.crashLocalVoltDB(
                    "Couldn't find voltdb_logstrings resource bundle. Should be in voldb_logstrings.properties.",
                    true, e);
        }
        Logger.getRootLogger().setResourceBundle(rb);

        // Make the LogManager shutdown hook the last thing to be done,
        // so that we'll get logging from any other shutdown behavior.
        ShutdownHooks.registerShutdownHook(ShutdownHooks.VOLT_LOG4J, true,
                new Runnable() {
                    @Override
                    public void run() {
                        LogManager.shutdown();
                    }
                });
    }

    /*
     * Encoding for various log settings that will fit in 3 bits
     */
    public static final int all = 0;
    public static final int trace = 1;
    public static final int debug = 2;
    public static final int info = 3;
    public static final int warn = 4;
    public static final int error = 5;
    public static final int fatal = 6;
    public static final int off = 7;

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
    public void l7dlog(Level level, String key, Object[] params, Throwable t) {
        if (params == null) {
            m_logger.l7dlog(getPriorityForLevel(level), key, t);
        }
        else {
            m_logger.l7dlog(getPriorityForLevel(level), key, params, t);
        }
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
    public void addSimpleWriterAppender(StringWriter writer) {
        m_logger.addAppender(new org.apache.log4j.WriterAppender(new org.apache.log4j.SimpleLayout(), writer));
    }

    @Override
    public void setLevel(Level level) {
        m_logger.setLevel(getPriorityForLevel(level));
    }

    /**
     * Static method to change the Log4j config globally.
     * @param xmlConfig The text of a Log4j config file.
     */
    public static void configure(String xmlConfig) {
        DOMConfigurator configurator = new DOMConfigurator();
        StringReader sr = new StringReader(xmlConfig.trim());
        configurator.doConfigure(sr, LogManager.getLoggerRepository());
    }

}
