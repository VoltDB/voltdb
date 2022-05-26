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

package org.voltdb.exportclient;

import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;

public class ExportClientLogger {

    final static long SUPPRESS_INTERVAL = 10;
    final private VoltLogger m_logger = new VoltLogger("ExportClient");

    public ExportClientLogger() {
    }

    private void log(Level level, Throwable cause, String format, Object... args) {
        m_logger.rateLimitedLog(SUPPRESS_INTERVAL, level, cause, format, args);
    }

    public VoltLogger getLogger() {
        return m_logger;
    }

    public void trace(String format, Object...args) {
        if (m_logger.isTraceEnabled()) {
            log(Level.TRACE, null, format, args);
        }
    }

    public void debug(String format, Object...args) {
        if (m_logger.isDebugEnabled()) {
            log(Level.DEBUG, null, format, args);
        }
    }

    public void info(String format, Object...args) {
        if (m_logger.isInfoEnabled()) {
            log(Level.INFO, null, format, args);
        }
    }

    public void warn(String format, Object...args) {
        log(Level.WARN, null, format, args);
    }

    public void error(String format, Object...args) {
        log(Level.ERROR, null, format, args);
    }

    public void fatal(String format, Object...args) {
        log(Level.FATAL, null, format, args);
    }

    public void trace(String format, Throwable cause, Object...args) {
        if (m_logger.isTraceEnabled()) {
            log(Level.TRACE, cause, format, args);
        }
    }

    public void debug(String format, Throwable cause, Object...args) {
        if (m_logger.isDebugEnabled()) {
            log(Level.DEBUG, cause, format, args);
        }
    }

    public void info(String format, Throwable cause, Object...args) {
        if (m_logger.isInfoEnabled()) {
            log(Level.INFO, cause, format, args);
        }
    }

    public void warn(String format, Throwable cause, Object...args) {
        log(Level.WARN, cause, format, args);
    }

    public void error(String format, Throwable cause, Object...args) {
        log(Level.ERROR, cause, format, args);
    }

    public void fatal(String format, Throwable cause, Object...args) {
        log(Level.FATAL, cause, format, args);
    }
}
