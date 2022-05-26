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

#ifndef LOGGER_H_
#define LOGGER_H_
#include "LogDefs.h"
#include "LogProxy.h"
#include <string>
#include <common/debuglog.h>

namespace voltdb {

/**
 * A logger caches the current log level for a counterpart logger elsewhere and forwards log statements as necessary.
 */
class Logger {
    friend class LogManager;
public:

    /**
     * Constructor that initializes with logging at OFF and caches a reference to a log proxy where log statements
     * will be forwarded to.
     * @param proxy Log proxy where log statements should be forwarded to
     */
    inline Logger(LogProxy *proxy, LoggerId id) : m_level(LOGLEVEL_OFF), m_id(id), m_logProxy(proxy) {}

    /**
     * Check if a specific log level is loggable
     * @param level Level to check for loggability
     * @returns true if the level is loggable, false otherwise
     */
    inline bool isLoggable(LogLevel level) const {
        vassert(level != voltdb::LOGLEVEL_OFF && level != voltdb::LOGLEVEL_ALL); //: "Should never log as ALL or OFF";
        return (level >= m_level);
    }

    /**
     * Log a statement at the level specified in the template parameter.
     * @param level Log level to attempt to log the statement at
     * @param statement Statement to log
     */
    inline void log(const voltdb::LogLevel level, const std::string *statement) const {
        vassert(level != voltdb::LOGLEVEL_OFF && level != voltdb::LOGLEVEL_ALL); //: "Should never log as ALL or OFF";
        if (level >= m_level && m_logProxy != NULL) {
            m_logProxy->log( m_id, level, statement->c_str());
        }
    }

    /**
     * Log a statement at the level specified in the template parameter.
     * @param level Log level to attempt to log the statement at
     * @param statement null terminated UTF-8 string containg the statement to log
     */
    inline void log(const voltdb::LogLevel level, const char *statement) const {
        vassert(level != voltdb::LOGLEVEL_OFF && level != voltdb::LOGLEVEL_ALL); //: "Should never log as ALL or OFF";
        if (level >= m_level && m_logProxy != NULL) {
            m_logProxy->log( m_id, level, statement);
        }
    }

private:
    /**
     * Currently active log level containing a cached value of the log level of some logger elsewhere
     */
    LogLevel m_level;
    LoggerId m_id;

    /**
     * LogProxy that log statements will be forwarded to.
     */
    const LogProxy *m_logProxy;
};

}
#endif /* LOGGER_H_ */
