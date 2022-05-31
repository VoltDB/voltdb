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

package org.voltcore.utils;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;

/*
 * Wraps a VoltLogger to provide rate-limited logging.
 * A single RateLimitedLogger will not log more frequently
 * than its configured limit. This rate limitation is
 * independent of the message and log level.
 *
 * Alternative mechanisms are available directly from
 * VoltLogger; see rateLimitedLog, etc., for details.
 */
public class RateLimitedLogger {

    private volatile long m_lastLogTime = 0;

    private final long m_maxLogIntervalMillis;
    private final VoltLogger m_logger;
    private final Level m_level;

    /**
     * Rate-limited logger constructor.
     *
     * @param maxLogIntervalMillis rate limit in millisecs
     * @param logger a {@link VoltLogger}
     * @param level default logging {@link Level}
     */
     public RateLimitedLogger(long maxLogIntervalMillis, VoltLogger logger, Level level) {
        m_maxLogIntervalMillis = maxLogIntervalMillis;
        m_logger = logger;
        m_level = level; // default
    }

    /**
     * Log a rate-limited message using the default level
     * for this logger.
     *
     * @param message string to be logged
     * @param now current time (as millisecs)
     */
    public void log(String message, long now) {
        log(message, now, m_level);
    }

    /**
     * Log a rate-limited message with specified level. Message is
     * formatted by caller if arguments are required, which may add
     * unnecessary overhead if logging is not subsequently needed.
     *
     * @param message string to be logged
     * @param now current time (as millisecs)
     * @param level a logging {@link Level}
     */
    public void log(String message, long now, Level level) {
        if (now - m_lastLogTime > m_maxLogIntervalMillis) {
            synchronized (this) {
                if (now - m_lastLogTime > m_maxLogIntervalMillis) {
                    m_logger.log(level, message, null);
                    m_lastLogTime = now;
                }
            }
        }
    }

    /**
     * Log a rate-limited message. Delays the formatting of the message
     * until (and if) it is actually logged.
     *
     * @param now current time (as millisecs)
     * @param level a logging {@link Level}
     * @param cause evidentiary exception, possibly null
     * @param format a {@link String#format(String, Object...)} string format
     * @param args format arguments
     */
    public void log(long now, Level level, Throwable cause, String format, Object... args) {
        if (now - m_lastLogTime > m_maxLogIntervalMillis) {
            synchronized (this) {
                if (now - m_lastLogTime > m_maxLogIntervalMillis) {
                    if (m_logger.isEnabledFor(level)) {
                        m_logger.logFmt(level, cause, format, args);
                    }
                    m_lastLogTime = now;
                }
            }
        }
    }
}
