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

#include "SynchronizedThreadLock.h"

#include "common/debuglog.h"
#include "common/ExecuteWithMpMemory.h"
#include "common/executorcontext.hpp"
#include "common/ThreadLocalPool.h"

#include "storage/persistenttable.h"

#ifdef LINUX
#include <sys/syscall.h>
#endif


namespace voltdb {

// Initialized when executor context is created.
pthread_mutex_t SynchronizedThreadLock::s_sharedEngineMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t SynchronizedThreadLock::s_sharedEngineCondition;
pthread_cond_t SynchronizedThreadLock::s_wakeLowestEngineCondition;

// The Global Countdown Latch is critical to correctly coordinating between engine threads
// when replicated tables are updated. Therefore we use SITES_PER_HOST as a constant that
// the latch is (re)initialized to after each use of the CountDown latch. To ensure correct
// behavior, we pre-initialize SITES_PER_HOST to -1. Then we set it to 0 in
// globalInitOrCreateOncePerProcess (ExecutorContext). After that only the lowest site should
// set SITES_PER_HOST to the correct value.
//
// For unit tests (CPP or Java) that reuse the same ExecutionContext globals multiple times,
// the engines should be deallocated first (and the last engine to go away will deallocate
// the last ThreadPool, which in turn will set SITES_PER_HOST to 0).
// If globalDestroyOncePerProcess() is used, it must be done after the ThreadPool deallocation.
int32_t SynchronizedThreadLock::s_SITES_PER_HOST = -1;
int32_t SynchronizedThreadLock::s_globalTxnStartCountdownLatch = 0;

bool SynchronizedThreadLock::s_inSingleThreadMode = false;
const int32_t SynchronizedThreadLock::s_mpMemoryPartitionId = 65535;
#ifndef  NDEBUG
bool SynchronizedThreadLock::s_usingMpMemory = false;
bool SynchronizedThreadLock::s_holdingReplicatedTableLock = false;
#endif

SharedEngineLocalsType SynchronizedThreadLock::s_enginesByPartitionId;
EngineLocals SynchronizedThreadLock::s_mpEngine(true);

void SynchronizedUndoQuantumReleaseInterest::notifyQuantumRelease() {
    assert (!SynchronizedThreadLock::isInSingleThreadMode());
    SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
    {
        ExecuteWithMpMemory usingMpMemory;
        m_realInterest->notifyQuantumRelease();
     }
     SynchronizedThreadLock::signalLowestSiteFinished();
};

void SynchronizedDummyUndoQuantumReleaseInterest::notifyQuantumRelease() {
    assert (!SynchronizedThreadLock::isInSingleThreadMode());
    SynchronizedThreadLock::countDownGlobalTxnStartCount(false);
}

void SynchronizedThreadLock::create() {
    assert(s_SITES_PER_HOST == -1);
    s_SITES_PER_HOST = 0;
    pthread_cond_init(&s_sharedEngineCondition, 0);
    pthread_cond_init(&s_wakeLowestEngineCondition, 0);
}

void SynchronizedThreadLock::destroy() {
    pthread_cond_destroy(&s_sharedEngineCondition);
    pthread_cond_destroy(&s_wakeLowestEngineCondition);
    s_SITES_PER_HOST = -1;
}

void SynchronizedThreadLock::init(int32_t sitesPerHost, EngineLocals& newEngineLocals) {
    if (s_SITES_PER_HOST == 0) {
        s_SITES_PER_HOST = sitesPerHost;
        s_globalTxnStartCountdownLatch = s_SITES_PER_HOST;
    }
    if (*newEngineLocals.enginePartitionId != 16383) {
        s_enginesByPartitionId[*newEngineLocals.enginePartitionId] = newEngineLocals;
        if (newEngineLocals.context->getContextEngine()->isLowestSite()) {
            // We need the Replicated table memory before the MP Site is initialized so
            // Just track it in s_mpEngine.
            VOLT_DEBUG("Initializing memory pool for Replicated Tables on thread %d", ThreadLocalPool::getThreadPartitionId());
            assert(s_mpEngine.context == NULL);
            s_mpEngine.context = newEngineLocals.context;

            delete s_mpEngine.enginePartitionId;
            s_mpEngine.enginePartitionId = new int32_t(s_mpMemoryPartitionId);

            delete s_mpEngine.poolData;
            PoolsByObjectSize *pools = new PoolsByObjectSize();
            PoolPairType* refCountedPools = new PoolPairType(1, pools);
            s_mpEngine.poolData = refCountedPools;

            delete s_mpEngine.stringData;
            s_mpEngine.stringData = new CompactingStringStorage();

            delete s_mpEngine.allocated;
            s_mpEngine.allocated = new std::size_t(0);
        }
    }
}

void SynchronizedThreadLock::resetMemory(int32_t partitionId) {
    lockReplicatedResourceForInit();
    if (partitionId == s_mpMemoryPartitionId) {
        // This is being called twice. First when the lowestSite goes away and then
        // when the MP Sites Engine goes away but we use the first opportunity to
        // remove the Replicated table memory pools allocated on the lowest site
        // thread before the MP Site was initialized.
        if (s_mpEngine.context != NULL) {
            VOLT_DEBUG("Reset memory pool for Replicated Tables on thread %d", ThreadLocalPool::getThreadPartitionId());
            assert(s_mpEngine.poolData->first == 1);
            delete s_mpEngine.poolData->second;
            delete s_mpEngine.poolData;
            s_mpEngine.poolData = NULL;
            delete s_mpEngine.stringData;
            s_mpEngine.stringData = NULL;
            delete s_mpEngine.allocated;
            s_mpEngine.allocated = NULL;
            delete s_mpEngine.enginePartitionId;
            s_mpEngine.enginePartitionId = NULL;
            s_mpEngine.context = NULL;
#ifdef VOLT_POOL_CHECKING
            pthread_mutex_lock(&ThreadLocalPool::s_sharedMemoryMutex);
            ThreadLocalPool::SizeBucketMap_t& mapBySize = ThreadLocalPool::s_allocations[s_mpMemoryPartitionId];
            pthread_mutex_unlock(&ThreadLocalPool::s_sharedMemoryMutex);
            ThreadLocalPool::SizeBucketMap_t::iterator mapForAdd = mapBySize.begin();
            while (mapForAdd != mapBySize.end()) {
                ThreadLocalPool::AllocTraceMap_t& allocMap = mapForAdd->second;
                mapForAdd++;
                if (!allocMap.empty()) {
                    ThreadLocalPool::AllocTraceMap_t::iterator nextAlloc = allocMap.begin();
                    do {
#ifdef VOLT_TRACE_ALLOCATIONS
                        VOLT_ERROR("Missing deallocation for %p at:", nextAlloc->first);
                        nextAlloc->second->printLocalTrace();
                        delete nextAlloc->second;
#else
                        VOLT_ERROR("Missing deallocation for %p at:", *nextAlloc);
#endif
                        nextAlloc++;
                    } while (nextAlloc != allocMap.end());
                    allocMap.clear();
                }
                mapBySize.erase(mapBySize.begin());
            }
#endif
        }
    }
    else {
        EngineLocals& engine = s_enginesByPartitionId[partitionId];
        engine.poolData = NULL;
        engine.stringData = NULL;
        engine.allocated = NULL;
        engine.enginePartitionId = NULL;
        engine.context = NULL;
        s_enginesByPartitionId.erase(partitionId);
        if (s_enginesByPartitionId.empty()) {
            // Junit Tests that use ServerThread (or LocalCluster.setHasLocalServer(true)) will use the same
            // static ee globals from one test to the next so the last engine of the previous test should
            // set this static to 0 so that the first engine in the next test will reinitialize the site count
            // so that the countdown latch behaves correctly for tests with different site counts.
            s_SITES_PER_HOST = 0;
        }
    }
    unlockReplicatedResourceForInit();
}

bool SynchronizedThreadLock::countDownGlobalTxnStartCount(bool lowestSite) {
    VOLT_DEBUG("Entering countdown latch...");
    assert(s_globalTxnStartCountdownLatch > 0);
    assert(ThreadLocalPool::getEnginePartitionId() != 16383);
    assert(!isInSingleThreadMode());
    if (lowestSite) {
        pthread_mutex_lock(&s_sharedEngineMutex);
        if (--s_globalTxnStartCountdownLatch != 0) {
            pthread_cond_wait(&s_wakeLowestEngineCondition, &s_sharedEngineMutex);
        }
        pthread_mutex_unlock(&s_sharedEngineMutex);
        VOLT_DEBUG("Switching context to MP partition on thread %d", ThreadLocalPool::getThreadPartitionId());
        setIsInSingleThreadMode(true);
        return true;
    }
    else {
        VOLT_DEBUG("Waiting for MP partition work to complete on thread %d", ThreadLocalPool::getThreadPartitionId());
        pthread_mutex_lock(&s_sharedEngineMutex);
        if (--s_globalTxnStartCountdownLatch == 0) {
            pthread_cond_broadcast(&s_wakeLowestEngineCondition);
        }
        pthread_cond_wait(&s_sharedEngineCondition, &s_sharedEngineMutex);
        pthread_mutex_unlock(&s_sharedEngineMutex);
        VOLT_DEBUG("Other SP partition thread released on thread %d", ThreadLocalPool::getThreadPartitionId());
        assert(!isInSingleThreadMode());
        return false;
    }
}

void SynchronizedThreadLock::signalLowestSiteFinished() {
    pthread_mutex_lock(&s_sharedEngineMutex);
    s_globalTxnStartCountdownLatch = s_SITES_PER_HOST;
    VOLT_DEBUG("Restore context to lowest SP partition on thread %d", ThreadLocalPool::getThreadPartitionId());
    setIsInSingleThreadMode(false);
    pthread_cond_broadcast(&s_sharedEngineCondition);
    pthread_mutex_unlock(&s_sharedEngineMutex);
}

void SynchronizedThreadLock::addUndoAction(bool synchronized, UndoQuantum *uq, UndoReleaseAction* action,
        PersistentTable *table) {
    if (synchronized) {
        assert(isInSingleThreadMode());
        // For shared replicated table, in the same host site with lowest id
        // will create the actual undo action, other sites register a dummy
        // undo action as placeholder. Note that since we only touch quantum memory
        // we don't need to switch to the lowest site context when registering the undo action.
        BOOST_FOREACH (const SharedEngineLocalsType::value_type& enginePair, s_enginesByPartitionId) {
            UndoQuantum* currUQ = enginePair.second.context->getCurrentUndoQuantum();
            VOLT_DEBUG("Local undo quantum is %p; Other undo quantum is %p", uq, currUQ);
            UndoReleaseAction* undoAction;
            UndoQuantumReleaseInterest *releaseInterest = NULL;
            if (uq == currUQ) {
                undoAction = action->getSynchronizedUndoAction(currUQ);
                if (table) {
                    releaseInterest = table->getReplicatedInterest();
                }
            } else {
                undoAction = action->getDummySynchronizedUndoAction(currUQ);
                if (table) {
                    releaseInterest = table->getDummyReplicatedInterest();
                }
            }
            currUQ->registerUndoAction(undoAction, releaseInterest);
        }
    } else {
        uq->registerUndoAction(action, table);
    }
}

// Special call for before we initialize ThreadLocalPool partitionIds
void SynchronizedThreadLock::lockReplicatedResourceForInit() {
    pthread_mutex_lock(&s_sharedEngineMutex);
}

void SynchronizedThreadLock::unlockReplicatedResourceForInit() {
    pthread_mutex_unlock(&s_sharedEngineMutex);
}

void SynchronizedThreadLock::lockReplicatedResource() {
    // We don't expect to be single-threaded here, since we would be
    // calling the countdown latch instead.
    // This method is for locking write-access to replicated resources in
    // multi-threaded execution.
    assert(!isInSingleThreadMode());
    VOLT_DEBUG("Attempting to acquire replicated resource lock on engine %d...", ThreadLocalPool::getThreadPartitionId());
    pthread_mutex_lock(&s_sharedEngineMutex);
#ifndef  NDEBUG
    assert(! s_holdingReplicatedTableLock);
    s_holdingReplicatedTableLock = true;
#endif
    VOLT_DEBUG("Acquired replicated resource lock on engine %d.", ThreadLocalPool::getThreadPartitionId());
}

void SynchronizedThreadLock::unlockReplicatedResource() {
    VOLT_DEBUG("Releasing replicated resource lock on engine %d", ThreadLocalPool::getThreadPartitionId());
#ifndef  NDEBUG
    s_holdingReplicatedTableLock = false;
#endif
    pthread_mutex_unlock(&s_sharedEngineMutex);
}

#ifndef  NDEBUG
bool SynchronizedThreadLock::usingMpMemory() {
    return s_usingMpMemory;
}

void SynchronizedThreadLock::setUsingMpMemory(bool isUsingMpMemory) {
    assert(SynchronizedThreadLock::isInSingleThreadMode() || SynchronizedThreadLock::isHoldingResourceLock());
    s_usingMpMemory = isUsingMpMemory;
}
#endif

bool SynchronizedThreadLock::isInLocalEngineContext() {
    return ThreadLocalPool::getEnginePartitionId() == ThreadLocalPool::getThreadPartitionId();
}

bool SynchronizedThreadLock::isInSingleThreadMode() {
    return s_inSingleThreadMode;
}

void SynchronizedThreadLock::setIsInSingleThreadMode(bool value) {
    s_inSingleThreadMode = value;
}

#ifndef  NDEBUG
bool SynchronizedThreadLock::isHoldingResourceLock() {
    return s_holdingReplicatedTableLock;
}
#endif

void SynchronizedThreadLock::assumeMpMemoryContext() {
#ifndef  NDEBUG
    if (usingMpMemory()) {
        VOLT_ERROR_STACK();
        assert(!usingMpMemory());
    }
#endif
    // We should either be running on the lowest site thread (in the lowest site context) or
    // or be holding the replicated resource lock (Note: This could be a false positive if
    // a different thread happens to have the Replicated Resource Lock)
    assert(isInSingleThreadMode() || s_holdingReplicatedTableLock);
    ExecutorContext::assignThreadLocals(s_mpEngine);
#ifndef  NDEBUG
    setUsingMpMemory(true);
#endif
}

void SynchronizedThreadLock::assumeLowestSiteContext() {
#ifndef  NDEBUG
    setUsingMpMemory(false);
#endif
    ExecutorContext::assignThreadLocals(s_enginesByPartitionId.begin()->second);
}

void SynchronizedThreadLock::assumeLocalSiteContext() {
#ifndef  NDEBUG
    assert(usingMpMemory());
    setUsingMpMemory(false);
#endif
    ExecutorContext::assignThreadLocals(s_enginesByPartitionId.find(ThreadLocalPool::getThreadPartitionId())->second);
}

void SynchronizedThreadLock::assumeSpecificSiteContext(EngineLocals& eng) {
    assert(*eng.enginePartitionId != s_mpMemoryPartitionId);
#ifndef  NDEBUG
    setUsingMpMemory(false);
#endif
    ExecutorContext::assignThreadLocals(eng);
}

bool SynchronizedThreadLock::isLowestSiteContext() {
    return ExecutorContext::getExecutorContext() == s_enginesByPartitionId.begin()->second.context;
}

long int SynchronizedThreadLock::getThreadId() {
#ifdef LINUX
    return (long int)syscall(SYS_gettid);
#else
    return -1;
#endif
}

EngineLocals SynchronizedThreadLock::getMpEngine() {return s_mpEngine;}

void SynchronizedThreadLock::resetEngineLocalsForTest() {
    s_mpEngine = EngineLocals(true);
    s_enginesByPartitionId.clear();
    ExecutorContext::resetStateForTest();
}

void SynchronizedThreadLock::setEngineLocalsForTest(int32_t partitionId, EngineLocals mpEngine, SharedEngineLocalsType enginesByPartitionId) {
    s_mpEngine = mpEngine;
    s_enginesByPartitionId = enginesByPartitionId;
    ExecutorContext::assignThreadLocals(enginesByPartitionId[partitionId]);
    ThreadLocalPool::assignThreadLocals(enginesByPartitionId[partitionId]);
}

}
