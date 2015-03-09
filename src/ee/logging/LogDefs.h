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

#ifndef LOGDEFS_H_
#define LOGDEFS_H_

namespace voltdb {

/**
 * Identifiers for the various loggers that are made available inside the EE. The order and ordinals here
 * must match the array in org.voltdb.jni.EELoggers
 */
enum LoggerId {
    LOGGERID_SQL,
    LOGGERID_HOST,
    LOGGERID_INVALID
};

/**
 * Identifiers for the various supported log levels. 8 values fits in three bits and the values here
 * must match the values in org.voltdb.jni.EELoggers
 */
enum LogLevel {
    LOGLEVEL_ALL,
    LOGLEVEL_TRACE,
    LOGLEVEL_DEBUG,
    LOGLEVEL_INFO,
    LOGLEVEL_WARN,
    LOGLEVEL_ERROR,
    LOGLEVEL_FATAL,
    LOGLEVEL_OFF
};

}

#endif /* LOGDEFS_H_ */
