/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

#include <cassert>
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

#include "common/UndoReleaseAction.h"
#include "common/UndoQuantumReleaseInterest.h"

namespace voltdb {
struct EngineLocals;
class UndoQuantum;
typedef std::map<int32_t, EngineLocals> SharedEngineLocalsType;

class SynchronizedUndoReleaseAction : public UndoReleaseAction {
public:
    SynchronizedUndoReleaseAction(UndoReleaseAction *realAction) : m_realAction(realAction) {}
    virtual ~SynchronizedUndoReleaseAction() {
        delete m_realAction;
    }

    void undo();

    void release();

private:
    UndoReleaseAction *m_realAction;
};

class SynchronizedUndoOnlyAction : public UndoOnlyAction {
public:
    SynchronizedUndoOnlyAction(UndoOnlyAction *realAction) : m_realAction(realAction) {}
    virtual ~SynchronizedUndoOnlyAction() {
        delete m_realAction;
    }

    void undo();

private:
    UndoOnlyAction *m_realAction;
};

class SynchronizedDummyUndoReleaseAction : public UndoReleaseAction {
public:
    SynchronizedDummyUndoReleaseAction() { }
    virtual ~SynchronizedDummyUndoReleaseAction() { }

    void undo();

    void release();
};

class SynchronizedDummyUndoOnlyAction : public UndoOnlyAction {
public:
    SynchronizedDummyUndoOnlyAction() { }
    virtual ~SynchronizedDummyUndoOnlyAction() { }

    void undo();
};

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

class SynchronizedThreadLock {
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

    static void lockReplicatedResourceNoThreadLocals();
    static void unlockReplicatedResourceNoThreadLocals();
    static void lockReplicatedResource();
    static void unlockReplicatedResource();

    static void addUndoAction(bool synchronized, UndoQuantum *uq, UndoReleaseAction* action,
            PersistentTable *interest = NULL);

    static bool isInSingleThreadMode();
    static bool isInLocalEngineContext();
#ifndef  NDEBUG
    static bool usingMpMemory();
    static bool isHoldingResourceLock();
#endif
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
    static void setEngineLocalsForTest(EngineLocals mpEngine, SharedEngineLocalsType enginesByPartitionId);
    static EngineLocals getMpEngineForTest();

private:
    static bool s_inSingleThreadMode;
#ifndef  NDEBUG
    static bool s_usingMpMemory;
    static bool s_holdingReplicatedTableLock;
#endif
    static pthread_mutex_t s_sharedEngineMutex;
    static pthread_cond_t s_sharedEngineCondition;
    static pthread_cond_t s_wakeLowestEngineCondition;
    static int32_t s_globalTxnStartCountdownLatch;
    static int32_t s_SITES_PER_HOST;
    static EngineLocals s_mpEngine;

public:
    static SharedEngineLocalsType s_enginesByPartitionId;
    static const int32_t s_mpMemoryPartitionId;
};

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
    ConditionalSynchronizedExecuteWithMpMemory(bool needMpMemoryOnLowestThread,
            bool isLowestSite, int64_t& exceptionTracker);
    ~ConditionalSynchronizedExecuteWithMpMemory();
    bool okToExecute() {return m_okToExecute; }

private:
    bool m_usingMpMemoryOnLowestThread;
    bool m_okToExecute;
};

}


#endif
