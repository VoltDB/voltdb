/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "TempTableLimits.h"

#include "common/SQLException.h"
#include "logging/LogManager.h"

#include <cstdio>

using namespace voltdb;

TempTableLimits::TempTableLimits()
    : m_currMemoryInBytes(0),
      m_logThreshold(-1),
      m_memoryLimit(1024 * 1024 * 100),
      m_logLatch(false)
{
}

void
TempTableLimits::reduceAllocated(int bytes)
{
    m_currMemoryInBytes -= bytes;
    if (m_currMemoryInBytes < m_logThreshold)
    {
        m_logLatch = false;
    }
}

void
TempTableLimits::increaseAllocated(int bytes)
{
    m_currMemoryInBytes += bytes;
    if (m_memoryLimit > 0 &&
        m_currMemoryInBytes > m_memoryLimit)
    {
        int limit_mb = static_cast<int>(m_memoryLimit / (1024 * 1024));
        char msg[1024];
        snprintf(msg, 1024,
                 "More than %d MB of temp table memory used while executing SQL.  Aborting.",
                 limit_mb);
        throw SQLException(SQLException::volt_temp_table_memory_overflow,
                           msg);
    }
    if (!m_logLatch && m_logThreshold > 0 &&
        m_currMemoryInBytes > m_logThreshold)
    {
        m_logLatch = true;
        int thresh_mb = static_cast<int>(m_logThreshold / (1024 * 1024));
        char msg[1024];
        snprintf(msg, 1024,
                 "More than %d MB of temp table memory used while executing SQL.  This may indicate an operation that should be broken into smaller chunks.",
                 thresh_mb);
        LogManager::getThreadLogger(LOGGERID_SQL)->log(LOGLEVEL_INFO, msg);
    }
}

const int64_t
TempTableLimits::getAllocated() const
{
    return m_currMemoryInBytes;
}

void
TempTableLimits::setLogThreshold(int64_t threshold)
{
    m_logThreshold = threshold;
}

const int64_t
TempTableLimits::getLogThreshold() const
{
    return m_logThreshold;
}

void
TempTableLimits::setMemoryLimit(int64_t limit)
{
    m_memoryLimit = limit;
}

const int64_t
TempTableLimits::getMemoryLimit() const
{
    return m_memoryLimit;
}
