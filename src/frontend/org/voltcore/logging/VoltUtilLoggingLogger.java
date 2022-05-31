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

import java.util.logging.Logger;

/**
 * Implements the core logging functionality for VoltLogger specific to
 * java.ulil.logging.
 *
 */
public class VoltUtilLoggingLogger implements VoltLogger.CoreVoltLogger {

    /**
     * Convert the VoltLogger Level to the java.ulil.logging Level
     */
    static java.util.logging.Level getPriorityForLevel(Level level) {
        switch  (level) {
            case DEBUG:
                return java.util.logging.Level.FINEST;
            case ERROR:
                return java.util.logging.Level.SEVERE;
            case FATAL:
                return java.util.logging.Level.SEVERE;
            case INFO:
                return java.util.logging.Level.INFO;
            case TRACE:
                return java.util.logging.Level.FINER;
            case WARN:
                return java.util.logging.Level.WARNING;
            default:
                return null;
        }
    }

    /** Underlying java.ulil.logging logger */
    Logger m_logger;

    VoltUtilLoggingLogger(String classname) {
        m_logger = Logger.getLogger(classname);
        if (m_logger == null)
            throw new RuntimeException("Unable to get java.util.logging.Logger instance.");
    }

    @Override
    public boolean isEnabledFor(Level level) {
        return m_logger.isLoggable(getPriorityForLevel(level));
    }

    @Override
    public void log(Level level, Object message, Throwable t) {
        String msg = "NULL";
        if (message != null)
            msg = message.toString();
        if (t != null)
            msg += " : Throwable: " + t.toString();
        m_logger.log(getPriorityForLevel(level), msg);
    }

    @Override
    public long getLogLevels(VoltLogger[] loggers) {
        System.err.printf("This logger doesn't support getting log levels. You need Log4j.\n");
        return 0;
    }

    @Override
    public void setLevel(Level level) {
        m_logger.setLevel(getPriorityForLevel(level));
    }

}
