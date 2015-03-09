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

package org.voltcore.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.cache.Cache;
import com.google_voltpatches.common.cache.CacheBuilder;
import java.util.IllegalFormatConversionException;
import java.util.MissingFormatArgumentException;

/*
 * Log a message to the specified logger, but limit the rate at which the message is logged.
 *
 * You can technically feed this thing nanoseconds and it will work
 */
public class RateLimitedLogger {

    private volatile long m_lastLogTime = 0;

    private final long m_maxLogIntervalMillis;

    private final VoltLogger m_logger;
    private final Level m_level;

    public RateLimitedLogger(long maxLogIntervalMillis, VoltLogger logger, Level level) {
        m_maxLogIntervalMillis = maxLogIntervalMillis;
        m_logger = logger;
        m_level = level;
    }

    public void log(String message, long now) {
        log(message, now, m_level);
    }

    public void log(String message, long now, Level level) {
        if (now - m_lastLogTime > m_maxLogIntervalMillis) {
            synchronized (this) {
                if (now - m_lastLogTime > m_maxLogIntervalMillis) {
                    switch(level) {
                        case DEBUG:
                            m_logger.debug(message); break;
                        case ERROR:
                            m_logger.error(message); break;
                        case FATAL:
                            m_logger.fatal(message); break;
                        case INFO:
                            m_logger.info(message); break;
                        case TRACE:
                            m_logger.trace(message); break;
                        case WARN:
                            m_logger.warn(message); break;
                    }
                    m_lastLogTime = now;
                }
            }
        }
    }

    private static final Cache<String, RateLimitedLogger> m_loggersCached =
            CacheBuilder.newBuilder().maximumSize(1000).build();

    /*
     * It is a very bad idea to use this when the message changes all the time.
     * It's also a cache and only makes a best effort to enforce the rate limit.
     */
    public static void tryLogForMessage(long now,
            final long maxLogInterval,
            final TimeUnit maxLogIntervalUnit,
            final VoltLogger logger,
            final Level level, String format, Object... parameters) {
        Callable<RateLimitedLogger> builder = new Callable<RateLimitedLogger>() {
            @Override
            public RateLimitedLogger call() throws Exception {
                return new RateLimitedLogger(maxLogIntervalUnit.toMillis(maxLogInterval), logger, level);
            }
        };

        final RateLimitedLogger rll;
        try {
            rll = m_loggersCached.get(format, builder);
            rll.log(String.format(format, parameters), now, level);
        } catch (MissingFormatArgumentException ex) {
            Throwables.propagate(Throwables.getRootCause(ex));
        } catch (IllegalFormatConversionException ex) {
            Throwables.propagate(Throwables.getRootCause(ex));
        } catch (ExecutionException ex) {
            Throwables.propagate(Throwables.getRootCause(ex));
        }
    }
}
