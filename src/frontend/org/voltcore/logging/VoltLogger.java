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

import com.google_voltpatches.common.base.Throwables;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.ShutdownHooks;

import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;

/**
 * Class that implements the core functionality of a Log4j logger
 * or a java.util.logging logger. The point is that it should work
 * whether log4j is in the classpath or not. New VoltDB code should
 * avoid log4j wherever possible.
 */
public class VoltLogger {
    final CoreVoltLogger m_logger;

    private static final String m_threadName = "Async Logger";
    private static final ExecutorService m_es = CoreUtils.getSingleThreadExecutor(m_threadName);

    private static final boolean m_disableAsync = Boolean.getBoolean("DISABLE_ASYNC_LOGGING");

    static {
        ShutdownHooks.registerShutdownHook(ShutdownHooks.VOLT_LOGGER, true, new Runnable() {
            @Override
            public void run() {
                try {
                    m_es.submit(new Runnable() {
                        @Override
                    public void run() {}
                    }).get();
                } catch (Exception e) {
                    Throwables.getRootCause(e).printStackTrace();
                }
            }
        });
    }


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

    /*
     * Submit all tasks asynchronously to the thread to preserve message order,
     * but don't wait for the task to complete for info, debug, trace, and warn
     */
    private void submit(final Level l, final Object message, final Throwable t, boolean wait) {
        if (m_disableAsync) {
            m_logger.log(l, message, t);
            return;
        }

        if (!m_logger.isEnabledFor(l)) return;

        final Thread currentThread = Thread.currentThread();
        final Runnable r = new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName(currentThread.getName());
                try {
                    m_logger.log(l, message, t);
                } finally {
                    Thread.currentThread().setName(m_threadName);
                }
            }
        };
        if (wait) {
            try {
                m_es.submit(r).get();
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        } else {
            m_es.execute(r);
        }
    }

    private void submitl7d(final Level level, final String key, final Object[] params, final Throwable t) {
        if (m_disableAsync) {
            m_logger.l7dlog(level, key, params, t);
            return;
        }

        if (!m_logger.isEnabledFor(level)) return;

        final Thread currentThread = Thread.currentThread();
        final Runnable r = new Runnable() {
            @Override
            public void run() {
                Thread.currentThread().setName(currentThread.getName());
                try {
                    m_logger.l7dlog(level, key, params, t);
                } finally {
                    Thread.currentThread().setName(m_threadName);
                }
            }
        };
        switch (level) {
            case INFO:
            case WARN:
            case DEBUG:
            case TRACE:
                m_es.execute(r);
                break;
            case FATAL:
            case ERROR:
                try {
                    m_es.submit(r).get();
                } catch (Exception e) {
                    Throwables.propagate(e);
                }
                break;
            default:
                throw new AssertionError("Unrecognized level " + level);
        }
    }

    public void debug(Object message) {
        submit(Level.DEBUG, message, null, false);
    }

    public void debug(Object message, Throwable t) {
        submit(Level.DEBUG, message, t, false);
    }

    public boolean isDebugEnabled() {
        return m_logger.isEnabledFor(Level.DEBUG);
    }

    public void error(Object message) {
        submit(Level.ERROR, message, null, true);
    }

    public void error(Object message, Throwable t) {
        submit(Level.ERROR, message, t, true);
    }

    public void fatal(Object message) {
        submit(Level.FATAL, message, null, true);
    }

    public void fatal(Object message, Throwable t) {
        submit(Level.FATAL, message, t, true);
    }

    public void info(Object message) {
        submit(Level.INFO, message, null, false);
    }

    public void info(Object message, Throwable t) {
        submit(Level.INFO, message, t, false);
    }

    public boolean isInfoEnabled() {
        return m_logger.isEnabledFor(Level.INFO);
    }

    public void trace(Object message) {
       submit(Level.TRACE, message, null, false);
    }

    public void trace(Object message, Throwable t) {
       submit(Level.TRACE, message, t, false);
    }

    public boolean isTraceEnabled() {
        return m_logger.isEnabledFor(Level.TRACE);
    }

    public void warn(Object message) {
        submit(Level.WARN, message, null, false);
    }

    public void warn(Object message, Throwable t) {
        submit(Level.WARN, message, t, false);
    }

    public void l7dlog(final Level level, final String key, final Throwable t) {
        submitl7d(level, key, null, t);
    }

    public void l7dlog(final Level level, final String key, final Object[] params, final Throwable t) {
        submitl7d(level, key, params, t);
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
            Class<?> loggerClz = Class.forName("org.voltcore.logging.VoltLog4jLogger");
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
            Class<?> loggerClz = Class.forName("org.voltcore.logging.VoltLog4jLogger");
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
