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

#ifndef LOGMANAGER_H_
#define LOGMANAGER_H_
#include "Logger.h"
#include "LogDefs.h"
#include "LogProxy.h"
#include <stdint.h>
#include <iostream>
#include <pthread.h>


namespace voltdb {

/**
 * A LogManager contains a hard coded set of loggers that have counterpart loggers elsewhere.
 */
class LogManager {
public:

    /**
     * Constructor that initializes all the loggers with the specified proxy
     * @param proxy The LogProxy that all the loggers should use
     */
    LogManager(LogProxy *proxy);

    /**
     * Retrieve a logger by ID
     * @parameter loggerId ID of the logger to retrieve
     */
    inline const Logger* getLogger(LoggerId id) const {
        switch (id) {
        case LOGGERID_SQL:
            return &m_sqlLogger;
        case LOGGERID_HOST:
            return &m_hostLogger;
        default:
            return NULL;
        }
    }

    /**
     * Update the log levels of the loggers.
     * @param logLevels Integer contaning the log levels for the various loggers
     */
    inline void setLogLevels(int64_t logLevels) {
        m_sqlLogger.m_level = static_cast<LogLevel>((7 & logLevels));
        m_hostLogger.m_level = static_cast<LogLevel>(((7 << 3) & logLevels) >> 3);
    }

    /**
     * Retrieve the log proxy used by this LogManager and its Loggers
     * @return LogProxy Pointer to the LogProxy in use by this LogManager and its Loggers
     */
    inline const LogProxy* getLogProxy() {
        return m_proxy;
    }

    /**
     * Frees the log proxy
     */
    ~LogManager() {
        delete m_proxy;
    }


    /**
     * Retrieve a logger by ID from the LogManager associated with this thread.
     * @parameter loggerId ID of the logger to retrieve
     */
    inline static const Logger* getThreadLogger(LoggerId id) {
        return getThreadLogManager()->getLogger(id);
    }

    inline static const LogLevel getLogLevel(LoggerId id) {
        return (LogLevel)getThreadLogManager()->getLogger(id)->m_level;
    }
private:

    static LogManager* getThreadLogManager();

    /**
     * The log proxy in use by this LogManager and its Loggers
     */
    const LogProxy *m_proxy;
    Logger m_sqlLogger;
    Logger m_hostLogger;
};
}
#endif /* LOGMANAGER_H_ */
