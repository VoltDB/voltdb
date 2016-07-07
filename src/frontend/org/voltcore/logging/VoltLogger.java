/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;

import com.google_voltpatches.common.base.Throwables;

/**
 * Class that implements the core functionality of a Log4j logger
 * or a java.util.logging logger. The point is that it should work
 * whether log4j is in the classpath or not. New VoltDB code should
 * avoid log4j wherever possible.
 */
public class VoltLogger {
    final CoreVoltLogger m_logger;
    private static final String ASYNCH_LOGGER_THREAD_NAME = "Async Logger";

    /// A thread factory implementation customized for asynchronous logging tasks.
    /// This class and its use to initialize m_asynchLoggerPool bypasses similar code in CoreUtils.
    /// Duplicating that functionality seemed less bad than maintaining potentially circular
    /// dependencies between the static initializers here and those in CoreUtils.
    /// Such dependencies were implicated in a mysterious platform-specific hang.
    /// Also, the CoreUtils code will LOG any exception thrown by the launched runnable.
    /// That seems like a bad idea in this case, since the failed runnable in question is
    /// responsible for logging.
    /// Here, for simplicity, the runnables provided by the callers of submit or execute
    /// take responsibility for their own blanket exception catching.
    /// They fall back to writing a complaint to System.err on the assumption that logging
    /// is seriously broken and unusable.
    private static class LoggerThreadFactory implements ThreadFactory {
        private static final int SMALL_STACK_SIZE = 1024 * 256;
        @Override
        public synchronized Thread newThread(final Runnable runnable) {
            Thread t = new Thread(null, runnable, ASYNCH_LOGGER_THREAD_NAME, SMALL_STACK_SIZE);
            t.setDaemon(true);
            return t;
        }
    };

    // The pool containing the logger thread(s) or null if asynch logging is disabled so that
    // all logging takes place synchronously on the caller's thread.
    private static ExecutorService m_asynchLoggerPool =
            Boolean.getBoolean("DISABLE_ASYNC_LOGGING") ?
                    null :
                    new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
                            new LoggerThreadFactory());

    /// ShutdownHooks calls shutdownAsynchronousLogging when it is finished running its hooks
    /// just before executing its final action -- which is typically to shutdown Log4J logging.
    /// Since ShutdownHooks potentially makes some (minimal) use of logging during its
    /// shutdown task processing, so it already has a VoltLogger dependency, hard-coding a
    /// direct call to shutdownAsynchronousLogging is simpler than trying to register this step
    /// as a generic high-priority shutdown hook.
    public static synchronized void shutdownAsynchronousLogging() {
        if (m_asynchLoggerPool != null) {
            try {
                // Submit and wait on an empty logger task to flush the queue.
                m_asynchLoggerPool.submit(new Runnable() {
                    @Override
                    public void run() {}
                }).get();
            } catch (Exception e) {
                Throwables.getRootCause(e).printStackTrace();
            }
            // Any logging that falls after the official shutdown flush of the
            // asynch logger can just fall back to synchronous on the caller thread.
            m_asynchLoggerPool.shutdown();
            try {
                m_asynchLoggerPool.awaitTermination(365, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                throw new RuntimeException("Unable to shutdown VoltLogger", e);
            }
            m_asynchLoggerPool = null;
        }
    }

    public static synchronized void startAsynchronousLogging(){
        if (m_asynchLoggerPool == null && !Boolean.getBoolean("DISABLE_ASYNC_LOGGING")) {
            m_asynchLoggerPool = new ThreadPoolExecutor(
               1, 1, 0L, TimeUnit.MILLISECONDS,
               new LinkedBlockingQueue<Runnable>(),
               new LoggerThreadFactory());
            try {
                m_asynchLoggerPool.submit(new Runnable() {
                    @Override
                    public void run() {}
                }).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Unable to prime asynchronous logging", e);
            }
        }
    }

    /**
     * Abstraction of core functionality shared between Log4j and
     * java.util.logging.
     */
    static interface CoreVoltLogger {
        public boolean isEnabledFor(Level level);
        public void log(Level level, Object message, Throwable t);
        public void l7dlog(Level level, String key, Object[] params, Throwable t);
        public long getLogLevels(VoltLogger loggers[]);
        public void setLevel(Level level);
    }

    /*
     * Submit a task asynchronously to the thread to preserve message order,
     * and wait for the task to complete.
     */
    private void submit(final Level level, final Object message, final Throwable t) {
        if (!m_logger.isEnabledFor(level)) return;

        if (m_asynchLoggerPool == null) {
            m_logger.log(level, message, t);
            return;
        }

        final Runnable runnableLoggingTask = createRunnableLoggingTask(level, message, t);
        try {
            m_asynchLoggerPool.submit(runnableLoggingTask).get();
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    /*
     * Submit a task asynchronously to the thread to preserve message order,
     * but don't wait for the task to complete for info, debug, trace, and warn
     */
    private void execute(final Level level, final Object message, final Throwable t) {
        if (!m_logger.isEnabledFor(level)) return;

        if (m_asynchLoggerPool == null) {
            m_logger.log(level, message, t);
            return;
        }

        final Runnable runnableLoggingTask = createRunnableLoggingTask(level, message, t);
        m_asynchLoggerPool.execute(runnableLoggingTask);
    }

    /**
     * Generate a runnable task that logs one message in an exception-safe way.
     * @param level
     * @param message
     * @param t
     * @param callerThreadName
     * @return
     */
    private Runnable createRunnableLoggingTask(final Level level,
            final Object message, final Throwable t) {
        // While logging, the logger thread temporarily disguises itself as its caller.
        final String callerThreadName = Thread.currentThread().getName();

        final Runnable runnableLoggingTask = new Runnable() {
            @Override
            public void run() {
                Thread loggerThread = Thread.currentThread();
                loggerThread.setName(callerThreadName);
                try {
                    m_logger.log(level, message, t);
                } catch (Throwable t) {
                    System.err.println("Exception thrown in logging thread for " +
                            callerThreadName + ":" + t);
                } finally {
                    loggerThread.setName(ASYNCH_LOGGER_THREAD_NAME);
                }
            }
        };
        return runnableLoggingTask;
    }

    private void submitl7d(final Level level, final String key, final Object[] params, final Throwable t) {
        if (!m_logger.isEnabledFor(level)) {
            return;
        }

        if (m_asynchLoggerPool == null) {
            m_logger.l7dlog(level, key, params, t);
            return;
        }

        final Runnable runnableLoggingTask = createRunnableL7dLoggingTask(level, key, params, t);
        switch (level) {
            case INFO:
            case WARN:
            case DEBUG:
            case TRACE:
                m_asynchLoggerPool.execute(runnableLoggingTask);
                break;
            case FATAL:
            case ERROR:
                try {
                    m_asynchLoggerPool.submit(runnableLoggingTask).get();
                } catch (Exception e) {
                    Throwables.propagate(e);
                }
                break;
            default:
                throw new AssertionError("Unrecognized level " + level);
        }
    }

    /**
     * Generate a runnable task that logs one localized message in an exception-safe way.
     * @param level
     * @param message
     * @param t
     * @param callerThreadName
     * @return
     */
    private Runnable createRunnableL7dLoggingTask(final Level level,
            final String key, final Object[] params, final Throwable t) {
        // While logging, the logger thread temporarily disguises itself as its caller.
        final String callerThreadName = Thread.currentThread().getName();

        final Runnable runnableLoggingTask = new Runnable() {
            @Override
            public void run() {
                Thread loggerThread = Thread.currentThread();
                loggerThread.setName(callerThreadName);
                try {
                    m_logger.l7dlog(level, key, params, t);
                } catch (Throwable t) {
                    System.err.println("Exception thrown in logging thread for " +
                            callerThreadName + ":" + t);
                } finally {
                    loggerThread.setName(ASYNCH_LOGGER_THREAD_NAME);
                }
            }
        };
        return runnableLoggingTask;
    }

    public void debug(Object message) {
        execute(Level.DEBUG, message, null);
    }

    public void debug(Object message, Throwable t) {
        execute(Level.DEBUG, message, t);
    }

    public boolean isDebugEnabled() {
        return m_logger.isEnabledFor(Level.DEBUG);
    }

    public void error(Object message) {
        submit(Level.ERROR, message, null);
    }

    public void error(Object message, Throwable t) {
        submit(Level.ERROR, message, t);
    }

    public void fatal(Object message) {
        submit(Level.FATAL, message, null);
    }

    public void fatal(Object message, Throwable t) {
        submit(Level.FATAL, message, t);
    }

    public void info(Object message) {
        execute(Level.INFO, message, null);
    }

    public void info(Object message, Throwable t) {
        execute(Level.INFO, message, t);
    }

    public boolean isInfoEnabled() {
        return m_logger.isEnabledFor(Level.INFO);
    }

    public void trace(Object message) {
        execute(Level.TRACE, message, null);
    }

    public void trace(Object message, Throwable t) {
        execute(Level.TRACE, message, t);
    }

    public boolean isTraceEnabled() {
        return m_logger.isEnabledFor(Level.TRACE);
    }

    public void warn(Object message) {
        execute(Level.WARN, message, null);
    }

    public void warn(Object message, Throwable t) {
        execute(Level.WARN, message, t);
    }

    public void l7dlog(final Level level, final String key, final Throwable t) {
        submitl7d(level, key, null, t);
    }

    public void l7dlog(final Level level, final String key, final Object[] params, final Throwable t) {
        submitl7d(level, key, params, t);
    }

    public long getLogLevels(VoltLogger loggers[]) {
        return m_logger.getLogLevels(loggers);
    }

    public void setLevel(Level level) {
        m_logger.setLevel(level);
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
        } catch (Exception e) {}
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

        // if unable to load Log4j, use java.util.logging
        if (tempLogger == null) {
            tempLogger = new VoltUtilLoggingLogger(classname);
        }
        // set the final variable for the core logger
        m_logger = tempLogger;
    }

    public void rateLimitedLog(long suppressInterval, Level level, Throwable cause, String format, Object...args) {
        RateLimitedLogger.tryLogForMessage(
                EstTime.currentTimeMillis(),
                suppressInterval, TimeUnit.SECONDS,
                this, level,
                cause, format, args
                );
    }
}
