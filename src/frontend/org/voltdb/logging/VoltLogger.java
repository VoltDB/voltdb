/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.logging;

import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Class that implements the core functionality of a Log4j logger
 * or a java.util.logging logger. The point is that it should work
 * whether log4j is in the classpath or not. New VoltDB code should
 * avoid log4j wherever possible.
 */
public class VoltLogger {
    final CoreVoltLogger m_logger;

    /**
     * Abstraction of core functionality shared between Log4j and
     * java.util.logging.
     */
    static interface CoreVoltLogger {
        public boolean isEnabledFor(Level level);
        public void log(Level level, Object message, Throwable t);
        public void l7dlog(Level level, String key, Object[] params, Throwable t);
        public void addSimpleWriterAppender(StringWriter writer);
        public void setLevel(Level level);
        public long getLogLevels(VoltLogger loggers[]);
    }

    public void debug(Object message) {
        m_logger.log(Level.DEBUG, message, null);
    }

    public void debug(Object message, Throwable t) {
        m_logger.log(Level.DEBUG, message, t);
    }

    public boolean isDebugEnabled() {
        return m_logger.isEnabledFor(Level.DEBUG);
    }

    public void error(Object message) {
        m_logger.log(Level.ERROR, message, null);
    }

    public void error(Object message, Throwable t) {
        m_logger.log(Level.ERROR, message, t);
    }

    public void fatal(Object message) {
        m_logger.log(Level.FATAL, message, null);
    }

    public void fatal(Object message, Throwable t) {
        m_logger.log(Level.FATAL, message, t);
    }

    public void info(Object message) {
        m_logger.log(Level.INFO, message, null);
    }

    public void info(Object message, Throwable t) {
        m_logger.log(Level.INFO, message, t);
    }

    public boolean isInfoEnabled() {
        return m_logger.isEnabledFor(Level.INFO);
    }

    public void trace(Object message) {
        m_logger.log(Level.TRACE, message, null);
    }

    public void trace(Object message, Throwable t) {
        m_logger.log(Level.TRACE, message, t);
    }

    public boolean isTraceEnabled() {
        return m_logger.isEnabledFor(Level.TRACE);
    }

    public void warn(Object message) {
        m_logger.log(Level.WARN, message, null);
    }

    public void warn(Object message, Throwable t) {
        m_logger.log(Level.WARN, message, t);
    }

    public void l7dlog(Level level, String key, Throwable t) {
        m_logger.l7dlog(level, key, null, t);
    }

    public void l7dlog(Level level, String key, Object[] params, Throwable t) {
        m_logger.l7dlog(level, key, params, t);
    }

    public void addSimpleWriterAppender(StringWriter writer) {
        m_logger.addSimpleWriterAppender(writer);
    }

    public void setLevel(Level level) {
        m_logger.setLevel(level);
    }

    public long getLogLevels(VoltLogger loggers[]) {
        return m_logger.getLogLevels(loggers);
    }

    /**
     * Static method to change the Log4j config globally. This fails
     * if you're not using Log4j for now.
     * @param xmlConfig The text of a Log4j config file.
     */
    public static void configure(String xmlConfig) {
        try {
            Class<?> loggerClz = Class.forName("org.voltdb.logging.VoltLog4jLogger");
            assert(loggerClz != null);
            Method configureMethod = loggerClz.getMethod("configure", String.class);
            configureMethod.invoke(null, xmlConfig);
        } catch (Exception e) {
            return;
        }
    }

    /**
     * Try to load the Log4j logger without importing it. Eventually support
     * graceful failback to java.util.logging.
     * @param classname The id of the logger.
     */
    public VoltLogger(String classname) {
        CoreVoltLogger tempLogger = null;

        // try to load the Log4j logger without importing it
        // any exception thrown will just keep going
        try {
            Class<?> loggerClz = Class.forName("org.voltdb.logging.VoltLog4jLogger");
            assert(loggerClz != null);

            Constructor<?> constructor = loggerClz.getConstructor(String.class);
            tempLogger = (CoreVoltLogger) constructor.newInstance(classname);
        }
        catch (Exception e) {}
        catch (LinkageError e) {}

        // if unable to load Log4j, try to use java.util.logging
        if (tempLogger == null)
            tempLogger = new VoltUtilLoggingLogger(classname);

        // set the final variable for the core logger
        m_logger = tempLogger;

        // Don't let the constructor exit without
        if (m_logger == null)
            throw new RuntimeException("Unable to get VoltLogger instance.");
    }
}
