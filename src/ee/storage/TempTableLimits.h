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

#pragma once

#include <stdint.h>

namespace voltdb {

/**
 * Track the amount of memory used by temp tables in a plan fragment's executors.
 * Log or throw exceptions based on thresholds.
 */
class TempTableLimits {
public:
    TempTableLimits(int64_t memoryLimit = 1024 * 1024 * 100, int64_t logThreshold = -1)
        : m_logThreshold(logThreshold) , m_memoryLimit(memoryLimit) { }

    /**
     * Track an increase in the amount of memory accumulated in temp tables.
     * Log once at INFO level to the SQL instance if the log threshold is set and it is crossed.
     * Throw a SQLException when the memory limit is exceeded.
     */
    void increaseAllocated(int bytes);
    void reduceAllocated(int bytes);

    int64_t getAllocated() const { return m_currMemoryInBytes; }
    int64_t getPeakMemoryInBytes() const { return m_peakMemoryInBytes; }
    void resetPeakMemory() { m_peakMemoryInBytes = m_currMemoryInBytes; }

private:
    /// The current amount of memory used by temp tables for this plan fragment.
    int64_t m_currMemoryInBytes = 0;
    /// The high water amount of memory used by temp tables
    /// during the current execution of this plan fragment.
    int64_t m_peakMemoryInBytes = 0;
    /// The memory allocation at which a log message will be generated.
    /// A negative value disables this behavior.
    const int64_t m_logThreshold;
    /// The memory allocation at which an exception will be thrown and the execution aborted.
    /// A negative value disables this behavior.
    const int64_t m_memoryLimit;
    /// True if we have already generated a log message for
    /// exceeding the log threshold and not yet dropped below it.
    bool m_logLatch = false;
};

} // namespace voltdb

