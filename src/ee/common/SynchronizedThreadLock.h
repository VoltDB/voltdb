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

#include <condition_variable>
#include <map>
#include <mutex>
#if __cplusplus >= 201103L
#include <atomic>
#else
#include <cstdatomic>
#endif

#include "debuglog.h"
#include "executorcontext.hpp"
#include "UndoQuantumReleaseInterest.h"

class DRBinaryLogTest;

namespace voltdb {
class UndoQuantum;
class UndoReleaseAction;
class PersistentTable;
using SharedEngineLocalsType = std::map<int32_t, EngineLocals>;

class SynchronizedUndoQuantumReleaseInterest : public UndoQuantumReleaseInterest {
public:
    SynchronizedUndoQuantumReleaseInterest(UndoQuantumReleaseInterest *realInterest) :
        m_realInterest(realInterest) {}
    virtual ~SynchronizedUndoQuantumReleaseInterest() { }

    void notifyQuantumRelease();

private:
    UndoQuantumReleaseInterest *m_realInterest;
};

class SynchronizedDummyUndoQuantumReleaseInterest : public UndoQuantumReleaseInterest {
public:
    SynchronizedDummyUndoQuantumReleaseInterest() { }
    virtual ~SynchronizedDummyUndoQuantumReleaseInterest() { }

    void notifyQuantumRelease();
};

namespace SynchronizedThreadLock {

    // For use only by friends:
    void lockReplicatedResource();
    void unlockReplicatedResource();

    // These methods are like the ones above, but are only
    // used during initialization and have fewer asserts.
    void lockReplicatedResourceForInit();
    void unlockReplicatedResourceForInit();

    // public:
    const int32_t s_mpMemoryPartitionId = 65535;
    static SharedEngineLocalsType s_enginesByPartitionId;

    void create();
    void destroy();
    void init(int32_t sitesPerHost, EngineLocals& newEngineLocals);
    void resetMemory(int32_t partitionId);

    /**
     * Cross-site synchronization functions
     */
    bool countDownGlobalTxnStartCount(bool lowestSite);
    void signalLowestSiteFinished();

    /**
     * Add a new undo action possibly in a synchronized manner.
     *
     * Wrapper around calling UndoQuantum::registerUndoAction
     */
    void addUndoAction(bool synchronized, UndoQuantum *uq, UndoReleaseAction* action,
            PersistentTable *interest = nullptr);
    void addTruncateUndoAction(bool synchronized, UndoQuantum *uq, UndoReleaseAction* action,
            PersistentTable *deletedTable);

    bool isInSingleThreadMode();
    void setIsInSingleThreadMode(bool value);
    bool isInLocalEngineContext();
    bool usingMpMemory();
    void setUsingMpMemory(bool isUsingMpMemory);
    bool isHoldingResourceLock();
    void debugSimulateSingleThreadMode(bool inSingleThreadMode);

    void assumeMpMemoryContext();
    void assumeLowestSiteContext();
    void assumeLocalSiteContext();
    bool isLowestSiteContext();
    void assumeSpecificSiteContext(EngineLocals& eng);

    long int getThreadId();
    void resetEngineLocalsForTest();
    void setEngineLocalsForTest(int32_t partitionId, EngineLocals const& mpEngine,
            SharedEngineLocalsType const& enginesByPartitionId);
    EngineLocals getMpEngine();
};

}

