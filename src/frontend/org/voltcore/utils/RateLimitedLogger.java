/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
 * Log a message to the specified logger, but limit the rate at which the message is logged.
 */
public class RateLimitedLogger {

    private volatile long m_lastLogTime = 0;

    private final int m_maxLogIntervalMillis;

    private final VoltLogger m_logger;
    private final Level m_level;

    public RateLimitedLogger(int maxLogIntervalMillis, VoltLogger logger, Level level) {
        m_maxLogIntervalMillis = maxLogIntervalMillis;
        m_logger = logger;
        m_level = level;
    }

    public void log(String message, long now) {
        if (now - m_lastLogTime > m_maxLogIntervalMillis) {
            synchronized (this) {
                if (now - m_lastLogTime > m_maxLogIntervalMillis) {
                    switch(m_level) {
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
}
