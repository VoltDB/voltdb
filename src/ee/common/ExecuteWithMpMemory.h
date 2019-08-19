/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

#include "common/executorcontext.hpp"
#include "common/SynchronizedThreadLock.h"
#include "common/types.h"

namespace voltdb {

class ExecuteWithMpMemory {
public:
    ExecuteWithMpMemory();
    ~ExecuteWithMpMemory();
};

class ConditionalExecuteWithMpMemory {
    bool m_usingMpMemory;
public:
    ConditionalExecuteWithMpMemory(bool needMpMemory);
    ~ConditionalExecuteWithMpMemory();
};


class ConditionalExecuteOutsideMpMemory {
    bool m_notUsingMpMemory;
public:
    ConditionalExecuteOutsideMpMemory(bool haveMpMemory);
    ~ConditionalExecuteOutsideMpMemory();
};

template<class T>
class ConditionalSynchronizedExecuteWithMpMemory {
    bool m_usingMpMemoryOnLowestThread;
    bool m_okToExecute;
public:
    ConditionalSynchronizedExecuteWithMpMemory(
            bool needMpMemoryOnLowestThread, bool isLowestSite,
            std::atomic<T>& tracker, const T& initValue) :
        m_usingMpMemoryOnLowestThread(needMpMemoryOnLowestThread && isLowestSite),
        m_okToExecute(!needMpMemoryOnLowestThread || m_usingMpMemoryOnLowestThread) {
            if (needMpMemoryOnLowestThread &&
                    SynchronizedThreadLock::countDownGlobalTxnStartCount(isLowestSite)) {
                VOLT_DEBUG("Entering UseMPmemory");
                SynchronizedThreadLock::assumeMpMemoryContext();
                // This must be done in here to avoid a race with the non-MP path.
                tracker.store(initValue);
            }
        }

    template<typename T2>
    ConditionalSynchronizedExecuteWithMpMemory(bool needMpMemoryOnLowestThread, bool isLowestSite,
            std::atomic<T>& val1, const T& initValue1, T2& val2, const T2& initValue2)
    : m_usingMpMemoryOnLowestThread(needMpMemoryOnLowestThread && isLowestSite)
    , m_okToExecute(!needMpMemoryOnLowestThread || m_usingMpMemoryOnLowestThread) {
        if (needMpMemoryOnLowestThread) {
            if (SynchronizedThreadLock::countDownGlobalTxnStartCount(isLowestSite)) {
                VOLT_DEBUG("Entering UseMPmemory");
                SynchronizedThreadLock::assumeMpMemoryContext();
                // This must be done in here to avoid a race with the non-MP path.
                val1 = initValue1;
                val2 = initValue2;
            }
        }
    }

    ~ConditionalSynchronizedExecuteWithMpMemory() {
        if (m_usingMpMemoryOnLowestThread) {
            VOLT_DEBUG("Switching to local site context and waking other threads...");
            SynchronizedThreadLock::assumeLocalSiteContext();
            SynchronizedThreadLock::signalLowestSiteFinished();
        }
    }
    inline bool okToExecute() const {
        return m_okToExecute;
    }
};

class ExecuteWithAllSitesMemory {
    const EngineLocals m_engineLocals;
#ifndef NDEBUG
    const bool m_wasUsingMpMemory;
#endif
public:
    ExecuteWithAllSitesMemory();
    ~ExecuteWithAllSitesMemory();
    SharedEngineLocalsType::iterator begin();
    SharedEngineLocalsType::iterator end();
};

class ScopedReplicatedResourceLock {
public:
    ScopedReplicatedResourceLock();
    ~ScopedReplicatedResourceLock();
};

} // end namespace voltdb

