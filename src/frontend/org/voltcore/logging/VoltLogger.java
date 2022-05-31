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

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltNullLogger.CoreNullLogger;

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
            // out for no async logging
            Boolean.getBoolean("DISABLE_ASYNC_LOGGING") ?
                    null :
                    // quick out for when we want no logging whatsoever
                    "true".equals(System.getProperty("voltdb_no_logging")) ?
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
        // quick out for when we want no logging whatsoever
        if ("true".equals(System.getProperty("voltdb_no_logging"))) {
            return;
        }

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

    /*
     * Abstraction of core functionality shared between Log4j and
     * java.util.logging.
     */
    static interface CoreVoltLogger {
        public boolean isEnabledFor(Level level);
        public void log(Level level, Object message, Throwable t);
        public long getLogLevels(VoltLogger[] loggers);
        public void setLevel(Level level);
    }

    /*
     * Submit a task asynchronously to the thread to preserve message order,
     * and wait for the task to complete. Used for error/fatal severities.
     *
     * Message formatting is done in the calling thread, as for the 'execute'
     * method, although here there is no hard requirement.
     */
    private void submit(Level level, Object message, Object[] args, Throwable t) {
        if (!m_logger.isEnabledFor(level)) {
            return;
        }

        if (args != null) {
            message = formatString(message, args);
        }

        if (m_asynchLoggerPool == null) {
            m_logger.log(level, message, t);
            return;
        }

        Runnable task = createRunnableLoggingTask(level, message, t);
        try {
            m_asynchLoggerPool.submit(task).get();
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    /*
     * Submit a task asynchronously to the thread to preserve message order,
     * but don't wait for the task to complete. Used for warn and lesser
     * severities.
     *
     * Message formatting is done in the calling thread since there is
     * no guarantee that the arguments are immutable.
     */
    private void execute(Level level, Object message, Object[] args, Throwable t) {
        if (!m_logger.isEnabledFor(level)) {
            return;
        }

        if (args != null) {
            message = formatString(message, args);
        }

        if (m_asynchLoggerPool == null) {
            m_logger.log(level, message, t);
            return;
        }

        Runnable task = createRunnableLoggingTask(level, message, t);
        try {
            m_asynchLoggerPool.execute(task);
        } catch (RejectedExecutionException e) {
            m_logger.log(Level.DEBUG, "Failed to execute logging task. Running in-line", e);
            task.run();
        }
    }

    /*
     * Safe string formatter
     */
    private String formatString(Object format, Object[] args) {
        try {
            return String.format(format.toString(), args);
        } catch (Exception ex1) {
            try {
                String err = String.format("Error formatting log message '%s' with arguments '%s'", format, Arrays.toString(args));
                m_logger.log(Level.ERROR, err, ex1);
                return format.toString(); // skip the arguments, we've logged them already
            } catch (Exception ex2) {
                try {
                    String err = String.format("Error formatting log message '%s'", format);
                    m_logger.log(Level.ERROR, err, ex2);
                    return format.toString(); // skip the arguments, they're not safe
                } catch (Exception ex3) {
                    // I think we've tried enough
                }
            }
        }
        return "Irrecoverable error formatting log message";
    }

    /*
     * Generate a runnable task that logs one message in an exception-safe way.
     */
    private Runnable createRunnableLoggingTask(final Level level,
                                               final Object message,
                                               final Throwable t) {
        // While logging, the logger thread temporarily disguises itself as its caller.
        final String callerThreadName = Thread.currentThread().getName();

        return new Runnable() {
            @Override
            public void run() {
                Thread loggerThread = Thread.currentThread();
                loggerThread.setName(callerThreadName);
                try {
                    m_logger.log(level, message, t);
                } catch (Throwable t2) {
                    System.err.printf("Exception thrown in logging thread for '%s': %s%n",
                                      callerThreadName, t2);
                } finally {
                    loggerThread.setName(ASYNCH_LOGGER_THREAD_NAME);
                }
            }
        };
    }

    /**
     * Public interface.  These methods execute logging asynchronously
     * to the caller but await completion before returning to the caller.
     *
     * There are 4 variants for each severity:
     * - simple string
     * - string formatting (note 'Fmt' in the method name)
     * - simple string with Throwable
     * - string formatting with Throwable
     *
     * The 'throwable' variants log the Throwable in some form, after
     * the main log message. The form used depends on the underlying
     * logger. With log4j, there is a stack trace.
     *
     * Severities: fatal, error.
     */

    public void fatal(Object message) {
        submit(Level.FATAL, message, null, null);
    }

    public void fatalFmt(String format, Object... args) {
        submit(Level.ERROR, format, args, null);
    }

    public void fatal(Object message, Throwable t) {
        submit(Level.FATAL, message, null, t);
    }

    public void fatalFmt(Throwable t, String format, Object... args) {
        submit(Level.ERROR, format, args, t);
    }

    public void error(Object message) {
        submit(Level.ERROR, message, null, null);
    }

    public void errorFmt(String format, Object... args) {
        submit(Level.ERROR, format, args, null);
    }

    public void error(Object message, Throwable t) {
        submit(Level.ERROR, message, null, t);
    }

    public void errorFmt(Throwable t, String format, Object... args) {
        submit(Level.ERROR, format, args, t);
    }

    /**
     * Public interface.  These methods execute logging asynchronously
     * to the caller and do not await completion.
     *
     * There are 4 variants for each severity:
     * - simple string
     * - string formatting (note 'Fmt' in the method name)
     * - simple string with Throwable
     * - string formatting with Throwable
     *
     * The 'throwable' variants log the Throwable in some form, after
     * the main log message. The form used depends on the underlying
     * logger. With log4j, there is a stack trace.
     *
     * Severities: warn, info, debug, trace.
     */

    public void warn(Object message) {
        execute(Level.WARN, message, null, null);
    }

    public void warnFmt(String format, Object... args) {
        execute(Level.WARN, format, args, null);
    }

    public void warn(Object message, Throwable t) {
        execute(Level.WARN, message, null, t);
    }

    public void warnFmt(Throwable t, String format, Object... args) {
        execute(Level.WARN, format, args, t);
    }

    public void info(Object message) {
        execute(Level.INFO, message, null, null);
    }

    public void infoFmt(String format, Object... args) {
        execute(Level.INFO, format, args, null);
    }

    public void info(Object message, Throwable t) {
        execute(Level.INFO, message, null, t);
    }

    public void infoFmt(Throwable t, String format, Object... args) {
        execute(Level.INFO, format, args, t);
    }

    public void debug(Object message) {
        execute(Level.DEBUG, message, null, null);
    }

    public void debugFmt(String format, Object... args) {
        execute(Level.DEBUG, format, args, null);
    }

    public void debug(Object message, Throwable t) {
        execute(Level.DEBUG, message, null, t);
    }

    public void debugFmt(Throwable t, String format, Object... args) {
        execute(Level.DEBUG, format, args, t);
    }

    public void trace(Object message) {
        execute(Level.TRACE, message, null, null);
    }

    public void traceFmt(String format, Object... args) {
        execute(Level.TRACE, format, args, null);
    }

    public void trace(Object message, Throwable t) {
        execute(Level.TRACE, message, null, t);
    }

    public void traceFmt(Throwable t, String format, Object... args) {
        execute(Level.TRACE, format, args, t);
    }

    /**
     * Variants where the logging level is supplied as an
     * argument rather than being implicit in the method name.
     */
    public void log(Level level, Object message, Throwable t) {
        logFmt(level, t, message, (Object[])null);
    }

    public void logFmt(Level level, String format, Object... args) {
        logFmt(level, (Throwable)null, format, args);
    }

    public void logFmt(Level level, Throwable t, Object message, Object... args) {
        switch (level) {
        case WARN:
        case INFO:
        case DEBUG:
        case TRACE:
            execute(level, message, args, t);
            break;
        case FATAL:
        case ERROR:
            submit(level, message, args, t);
            break;
        default:
            throw new AssertionError("Unrecognized level " + level);
        }
    }

    /**
     * Rate-limited logging for messages that are likely to recur frequently, and
     * for which we want to avoid filling the log with repeated messages.
     * Whether this is a "repeated" message is determined by the format string.
     * Message formatting is deferred until it is determined that the message
     * will in fact be logged.
     */
    public void rateLimitedLog(long suppressInterval, Level level, Throwable cause, String format, Object... args) {
        if (m_rateLimiter.shouldLog(format, suppressInterval * 1000)) {
            logFmt(level, cause, format, args);
        }
    }

    public void rateLimitedError(long suppressInterval, String format, Object... args) {
        rateLimitedLog(suppressInterval, Level.ERROR, null, format, args);
    }

    public void rateLimitedWarn(long suppressInterval, String format, Object... args) {
        rateLimitedLog(suppressInterval, Level.WARN, null, format, args);
    }

    public void rateLimitedInfo(long suppressInterval, String format, Object... args) {
        rateLimitedLog(suppressInterval, Level.INFO, null, format, args);
    }

    private static final LogRateLimiter m_rateLimiter = new LogRateLimiter();

    /**
     * Tests for whether the "non-error" severities are enabled
     * for logging. These can be used to avoid message-preparation
     * overhead for messages that will not be logged, but note
     * that the check is made in the logging methods regardless.
     * In particular, the methods that take a format string and
     * arguments will check the level before formatting. You still
     * might want to use these calls if argument evaluation is
     * expensive and the level is likely to be disabled.
     */
    public boolean isInfoEnabled() {
        return m_logger.isEnabledFor(Level.INFO);
    }

    public boolean isDebugEnabled() {
        return m_logger.isEnabledFor(Level.DEBUG);
    }

    public boolean isTraceEnabled() {
        return m_logger.isEnabledFor(Level.TRACE);
    }

    public boolean isEnabledFor(Level level) {
        return m_logger.isEnabledFor(level);
    }

    /**
     * Other general utilities
     */
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
     * @param voltroot The VoltDB root path
     */
    public static void configure(String xmlConfig, File voltroot) {
        try {
            Class<?> loggerClz = Class.forName("org.voltcore.logging.VoltLog4jLogger");
            assert(loggerClz != null);
            Method configureMethod = loggerClz.getMethod("configure", String.class, File.class);
            configureMethod.invoke(null, xmlConfig, voltroot);
        } catch (Exception e) {}
    }

    /**
     * Try to load the Log4j logger without importing it. Eventually support
     * graceful failback to java.util.logging.
     * @param classname The id of the logger.
     */
    public VoltLogger(String classname) {
        // quick out for when we want no logging whatsoever
        if ("true".equals(System.getProperty("voltdb_no_logging"))) {
            m_logger = new CoreNullLogger();
            return;
        }

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

    /**
     * Constructor used by VoltNullLogger
     */
    protected VoltLogger(CoreVoltLogger logger) {
        assert(logger != null);
        m_logger = logger;
    }

}
