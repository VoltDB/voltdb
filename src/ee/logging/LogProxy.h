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

#ifndef LOGPROXY_H_
#define LOGPROXY_H_
#include "LogDefs.h"
#include <string>

namespace voltdb {

/**
 * A log proxy sends log messages on behalf of loggers to the destination where the statements will be logged.
 */
class LogProxy {
public:

    /**
     * Log a statement on behalf of the specified logger at the specified log level
     * @param LoggerId ID of the logger that received this statement
     * @param level Log level of the statement
     * @param statement null terminated UTF-8 string containing the statement to log
     */
    virtual void log(LoggerId loggerId, LogLevel level, const char *statement) const = 0;

    /**
     * Destructor must be virtual so that LogProxy implementations can cleanup.
     */
    virtual ~LogProxy() {}
};

}
#endif /* LOGPROXY_H_ */
