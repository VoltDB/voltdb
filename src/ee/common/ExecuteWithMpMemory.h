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

#ifndef VOLTDB_EXECUTEWITHMPMEMORY_H
#define VOLTDB_EXECUTEWITHMPMEMORY_H

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
public:
    ConditionalExecuteWithMpMemory(bool needMpMemory);

    ~ConditionalExecuteWithMpMemory();

private:
    bool m_usingMpMemory;
};


class ConditionalExecuteOutsideMpMemory {
public:
    ConditionalExecuteOutsideMpMemory(bool haveMpMemory);

    ~ConditionalExecuteOutsideMpMemory();

private:
    bool m_notUsingMpMemory;
};

class ConditionalSynchronizedExecuteWithMpMemory {
public:
    ConditionalSynchronizedExecuteWithMpMemory(
            bool needMpMemoryOnLowestThread, bool isLowestSite, std::function<void()> initiator)
    : m_usingMpMemoryOnLowestThread(needMpMemoryOnLowestThread && isLowestSite)
    , m_okToExecute(!needMpMemoryOnLowestThread || m_usingMpMemoryOnLowestThread) {
        if (needMpMemoryOnLowestThread) {
            if (SynchronizedThreadLock::countDownGlobalTxnStartCount(isLowestSite)) {
                VOLT_DEBUG("Entering UseMPmemory");
                SynchronizedThreadLock::assumeMpMemoryContext();
                // This must be done in here to avoid a race with the non-MP path.
                initiator();
            }
        }
    }
    ~ConditionalSynchronizedExecuteWithMpMemory();
    bool okToExecute() const {
        return m_okToExecute;
    }
private:
    bool m_usingMpMemoryOnLowestThread;
    bool m_okToExecute;
};

class ExecuteWithAllSitesMemory {
public:
    ExecuteWithAllSitesMemory();

    ~ExecuteWithAllSitesMemory();

    SharedEngineLocalsType::iterator begin();
    SharedEngineLocalsType::iterator end();

private:
    const EngineLocals m_engineLocals;
#ifndef NDEBUG
    const bool m_wasUsingMpMemory;
#endif
};

class ScopedReplicatedResourceLock {
public:
    ScopedReplicatedResourceLock();
    ~ScopedReplicatedResourceLock();
};

class ConditionalExecuteWithMpMemoryAndScopedResourceLock {
public:
    ConditionalExecuteWithMpMemoryAndScopedResourceLock(bool needMpMemory);

    ~ConditionalExecuteWithMpMemoryAndScopedResourceLock();

private:
    bool m_usingMpMemory;
};
} // end namespace voltdb

#endif //VOLTDB_EXECUTEWITHMPMEMORY_H
