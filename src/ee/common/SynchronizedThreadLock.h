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

#ifndef SYNCHRONIZEDTHREADLOCK_H_
#define SYNCHRONIZEDTHREADLOCK_H_

//#include "boost/scoped_ptr.hpp"
//#include "boost/unordered_map.hpp"

#include <common/debuglog.h>
#include <map>
#include <stack>
#include <string>
#include <vector>
#include <pthread.h>
#if __cplusplus >= 201103L
#include <atomic>
#else
#include <cstdatomic>
#endif

#include "common/UndoQuantumReleaseInterest.h"

class DRBinaryLogTest;

namespace voltdb {
struct EngineLocals;
class UndoQuantum;
class UndoReleaseAction;
typedef std::map<int32_t, EngineLocals> SharedEngineLocalsType;

class SynchronizedUndoQuantumReleaseInterest : public UndoQuantumReleaseInterest {
public:
    SynchronizedUndoQuantumReleaseInterest(UndoQuantumReleaseInterest *realInterest) : m_realInterest(realInterest) {}
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

class PersistentTable;
class ExecuteWithAllSitesMemory;
class ReplicatedMaterializedViewHandler;

class SynchronizedThreadLock {

    friend class ExecuteWithAllSitesMemory;
    friend class ReplicatedMaterializedViewHandler;
    friend class ScopedReplicatedResourceLock;
    friend class VoltDBEngine;
    friend class ::DRBinaryLogTest;

public:
    static void create();
    static void destroy();
    static void init(int32_t sitesPerHost, EngineLocals& newEngineLocals);
    static void resetMemory(int32_t partitionId);

    /**
     * Cross-site synchronization functions
     */
    static bool countDownGlobalTxnStartCount(bool lowestSite);
    static void signalLowestSiteFinished();

    /**
     * Add a new undo action possibly in a synchronized manner.
     *
     * Wrapper around calling UndoQuantum::registerUndoAction
     */
    static void addUndoAction(bool synchronized, UndoQuantum *uq, UndoReleaseAction* action,
            PersistentTable *interest = NULL);
    static void addTruncateUndoAction(bool synchronized, UndoQuantum *uq, UndoReleaseAction* action,
            PersistentTable *deletedTable);

    static bool isInSingleThreadMode();
    static void setIsInSingleThreadMode(bool value);
    static bool isInLocalEngineContext();
    static bool usingMpMemory();
    static void setUsingMpMemory(bool isUsingMpMemory);
    static bool isHoldingResourceLock();
    static void debugSimulateSingleThreadMode(bool inSingleThreadMode) {
        s_inSingleThreadMode = inSingleThreadMode;
    }

    static void assumeMpMemoryContext();
    static void assumeLowestSiteContext();
    static void assumeLocalSiteContext();
    static bool isLowestSiteContext();
    static void assumeSpecificSiteContext(EngineLocals& eng);

    static long int getThreadId();
    static void resetEngineLocalsForTest();
    static void setEngineLocalsForTest(int32_t partitionId, EngineLocals mpEngine, SharedEngineLocalsType enginesByPartitionId);
    static EngineLocals getMpEngine();

private:

    // For use only by friends:
    static void lockReplicatedResource();
    static void unlockReplicatedResource();

    // These methods are like the ones above, but are only
    // used during initialization and have fewer asserts.
    static void lockReplicatedResourceForInit();
    static void unlockReplicatedResourceForInit();

    static bool s_inSingleThreadMode;
#ifndef  NDEBUG
    static bool s_usingMpMemory;
#endif
    static bool s_holdingReplicatedTableLock;
    static pthread_mutex_t s_sharedEngineMutex;
    static pthread_cond_t s_sharedEngineCondition;
    static pthread_cond_t s_wakeLowestEngineCondition;
    static int32_t s_globalTxnStartCountdownLatch;
    static int32_t s_SITES_PER_HOST;
    static EngineLocals s_mpEngine;
    static SharedEngineLocalsType s_enginesByPartitionId;

public:
    static const int32_t s_mpMemoryPartitionId;
};

}

#endif
