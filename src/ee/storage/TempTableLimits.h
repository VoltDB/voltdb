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

#ifndef _EE_STORAGE_TEMPTABLELIMITS_H_
#define _EE_STORAGE_TEMPTABLELIMITS_H_

#include <stdint.h>

namespace voltdb
{
    /**
     * Track the amount of memory used for the temp tables contained
     * within a plan fragment's executors.  Log or throw exceptions
     * based on thresholds.
     */
    class TempTableLimits
    {
    public:
        TempTableLimits();

        /**
         * Increase the amount of memory accumulated in temp tables.
         * Will log once at INFO level to the SQL instance if the log
         * threshold is set and it is crossed.  Will throw a
         * SQLException when the memory limit is exceeded.
         */
        void increaseAllocated(int bytes);
        void reduceAllocated(int bytes);

        const int64_t getAllocated() const;
        void setLogThreshold(int64_t threshold);
        const int64_t getLogThreshold() const;
        void setMemoryLimit(int64_t limit);
        const int64_t getMemoryLimit() const;

    private:
        // The current amount of memory used by temp tables for this
        // plan fragment
        int64_t m_currMemoryInBytes;
        // The memory allocation at which a log message will be
        // generated.  A negative value with disable this behavior.
        int64_t m_logThreshold;
        // The memory allocation at which an exception will be thrown
        // and the execution aborted.  A negative value will disable
        // this behavior.
        int64_t m_memoryLimit;
        // True if we have already generated a log message for
        // exceeding the log threshold and not yet dropped below it.
        bool m_logLatch;
    };
}


#endif // _EE_STORAGE_TEMPTABLELIMITS_H_
