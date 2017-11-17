/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
#include "common/executorcontext.hpp"
#include "common/debuglog.h"
#include "storage/persistenttable.h"
#include "common/ThreadLocalPool.h"

namespace voltdb {

// Initialized when executor context is created.
pthread_mutex_t SynchronizedThreadLock::s_sharedEngineMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t SynchronizedThreadLock::s_sharedEngineCondition;
pthread_cond_t SynchronizedThreadLock::s_wakeLowestEngineCondition;
int32_t SynchronizedThreadLock::s_globalTxnStartCountdownLatch = 0;
int32_t SynchronizedThreadLock::s_SITES_PER_HOST = -1;
bool SynchronizedThreadLock::s_inSingleThreadMode = false;
bool SynchronizedThreadLock::s_usingMpMemory = false;
SharedEngineLocalsType SynchronizedThreadLock::s_enginesByPartitionId;
EngineLocals SynchronizedThreadLock::s_mpEngine(true);

void SynchronizedUndoReleaseAction::undo() {
    if (!SynchronizedThreadLock::isInSingleThreadMode()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
        {
            ExecuteWithMpMemory usingMpMemory;
            m_realAction->undo();
        }
        SynchronizedThreadLock::signalLowestSiteFinished();
    } else {
        m_realAction->undo();
    }
}

void SynchronizedUndoReleaseAction::release() {
    if (!SynchronizedThreadLock::isInSingleThreadMode()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
        {
            ExecuteWithMpMemory usingMpMemory;
            m_realAction->release();
        }
        SynchronizedThreadLock::signalLowestSiteFinished();
    } else {
        m_realAction->release();
    }
}

void SynchronizedUndoOnlyAction::undo() {
    if (!SynchronizedThreadLock::isInSingleThreadMode()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
        {
            ExecuteWithMpMemory usingMpMemory;
            m_realAction->undo();
        }
        SynchronizedThreadLock::signalLowestSiteFinished();
    } else {
        m_realAction->undo();
    }
}

void SynchronizedDummyUndoReleaseAction::undo() {
    if (!SynchronizedThreadLock::isInSingleThreadMode()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(false);
    }
}

void SynchronizedDummyUndoReleaseAction::release() {
    if (!SynchronizedThreadLock::isInSingleThreadMode()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(false);
    }
}

void SynchronizedDummyUndoOnlyAction::undo() {
    if (!SynchronizedThreadLock::isInSingleThreadMode()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(false);
    }
}


void SynchronizedUndoQuantumReleaseInterest::notifyQuantumRelease() {
    if (!SynchronizedThreadLock::isInSingleThreadMode()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
        {
            ExecuteWithMpMemory usingMpMemory;
            m_realInterest->notifyQuantumRelease();
        }
        SynchronizedThreadLock::signalLowestSiteFinished();
    } else {
        m_realInterest->notifyQuantumRelease();
    }
};

void SynchronizedDummyUndoQuantumReleaseInterest::notifyQuantumRelease() {
    if (!SynchronizedThreadLock::isInSingleThreadMode()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(false);
    }
}

void SynchronizedThreadLock::create() {
    assert(s_SITES_PER_HOST == -1);
    s_SITES_PER_HOST = 0;
    pthread_mutex_init(&s_sharedEngineMutex, NULL);
    pthread_cond_init(&s_sharedEngineCondition, 0);
    pthread_cond_init(&s_wakeLowestEngineCondition, 0);
#ifdef VOLT_TRACE_ENABLED
    pthread_mutex_init(&ThreadLocalPool::s_sharedMemoryMutex, NULL);
#endif
}

void SynchronizedThreadLock::destroy() {
    pthread_cond_destroy(&s_sharedEngineCondition);
    pthread_cond_destroy(&s_wakeLowestEngineCondition);
    pthread_mutex_destroy(&s_sharedEngineMutex);
#ifdef VOLT_TRACE_ENABLED
    pthread_mutex_destroy(&ThreadLocalPool::s_sharedMemoryMutex);
#endif
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
            s_mpEngine.enginePartitionId = new int32_t(16383);
            s_mpEngine.poolData = new PoolPairType(1, new PoolsByObjectSize());
            s_mpEngine.stringData = new CompactingStringStorage();
            s_mpEngine.allocated = new std::size_t;
        }
    }
}

void SynchronizedThreadLock::resetMemory(int32_t partitionId) {
    SynchronizedThreadLock::lockReplicatedResourceNoThreadLocals();
    if (partitionId == 16383) {
        // This is being called twice. First when the lowestSite goes away and then
        // when the MP Sites Engine goes away but we use the first opportunity to
        // remove the Replicated table memory pools allocated on the lowest site
        // thread before the MP Site was initialized.
        if (s_mpEngine.context != NULL) {
            VOLT_TRACE("Reset memory pool for Replicated Tables on thread %d", ThreadLocalPool::getThreadPartitionId());
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
#ifdef VOLT_TRACE_ENABLED
            pthread_mutex_lock(&ThreadLocalPool::s_sharedMemoryMutex);
            ThreadLocalPool::SizeBucketMap_t& mapBySize = ThreadLocalPool::s_allocations[16383];
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
            s_SITES_PER_HOST = 0;
        }
    }
    SynchronizedThreadLock::unlockReplicatedResourceNoThreadLocals();
}

bool SynchronizedThreadLock::countDownGlobalTxnStartCount(bool lowestSite) {
    assert(s_globalTxnStartCountdownLatch > 0);
    assert(!s_inSingleThreadMode);
    if (lowestSite) {
        pthread_mutex_lock(&s_sharedEngineMutex);
        if (--s_globalTxnStartCountdownLatch != 0) {
            pthread_cond_wait(&s_wakeLowestEngineCondition, &s_sharedEngineMutex);
        }
        pthread_mutex_unlock(&s_sharedEngineMutex);
        VOLT_DEBUG("Switching context to MP partition on thread %d", ThreadLocalPool::getThreadPartitionId());
        s_inSingleThreadMode = true;
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
        assert(!s_inSingleThreadMode);
        VOLT_DEBUG("Other SP partition thread released on thread %d", ThreadLocalPool::getThreadPartitionId());
        return false;
    }
}

void SynchronizedThreadLock::signalLowestSiteFinished() {
    pthread_mutex_lock(&s_sharedEngineMutex);
    s_globalTxnStartCountdownLatch = s_SITES_PER_HOST;
    VOLT_DEBUG("Restore context to lowest SP partition on thread %d", ThreadLocalPool::getThreadPartitionId());
    s_inSingleThreadMode = false;
    pthread_cond_broadcast(&s_sharedEngineCondition);
    pthread_mutex_unlock(&s_sharedEngineMutex);
}

void SynchronizedThreadLock::addUndoAction(bool synchronized, UndoQuantum *uq, UndoReleaseAction* action,
        PersistentTable *table) {
    if (synchronized) {
        // For shared replicated table, in the same host site with lowest id
        // will create the actual undo action, other sites register a dummy
        // undo action as placeholder. Note that since we only touch quantum memory
        // we don't need to switch to the lowest site context when registering the undo action.
        BOOST_FOREACH (const SharedEngineLocalsType::value_type& enginePair, s_enginesByPartitionId) {
            UndoQuantum* currUQ = enginePair.second.context->getCurrentUndoQuantum();
            VOLT_DEBUG("Local undo quantum is %p; Other undo quantum is %p", uq, currUQ);
            UndoReleaseAction* undoAction;
            UndoQuantumReleaseInterest *releaseInterest = NULL;
            UndoOnlyAction* undoOnly = dynamic_cast<UndoOnlyAction*>(action);
            if (uq == currUQ) {
                // do the actual work
                if (undoOnly != NULL) {
                    undoAction = new (*currUQ)SynchronizedUndoOnlyAction(undoOnly);
                }
                else {
                    undoAction = new (*currUQ)SynchronizedUndoReleaseAction(action);
                }
                if (table) {
                    releaseInterest = table->getReplicatedInterest();
                }
            } else {
                // put a placeholder
                if (undoOnly != NULL) {
                    undoAction = new (*currUQ) SynchronizedDummyUndoOnlyAction();
                }
                else {
                    undoAction = new (*currUQ) SynchronizedDummyUndoReleaseAction();
                }
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
void SynchronizedThreadLock::lockReplicatedResourceNoThreadLocals() {
    pthread_mutex_lock(&s_sharedEngineMutex);
}

void SynchronizedThreadLock::unlockReplicatedResourceNoThreadLocals() {
    pthread_mutex_unlock(&s_sharedEngineMutex);
}

void SynchronizedThreadLock::lockReplicatedResource() {
    pthread_mutex_lock(&s_sharedEngineMutex);
    // Can't use threadlocals because this is called before we assign the partitionIds to threadlocals
    VOLT_DEBUG("Grabbing replicated resource lock on engine %d", ThreadLocalPool::getThreadPartitionId());
    if (s_inSingleThreadMode) {
        VOLT_ERROR_STACK();
        assert(false);
    }
//    assert(!s_inSingleThreadMode);
}

void SynchronizedThreadLock::unlockReplicatedResource() {
    VOLT_DEBUG("Releasing replicated resource lock on engine %d", ThreadLocalPool::getThreadPartitionId());
    pthread_mutex_unlock(&s_sharedEngineMutex);
}

bool SynchronizedThreadLock::isInLocalEngineContext() {
    return ThreadLocalPool::getEnginePartitionId() == ThreadLocalPool::getThreadPartitionId();
}

bool SynchronizedThreadLock::isInSingleThreadMode() {
    return s_inSingleThreadMode;
}

void SynchronizedThreadLock::assumeMpMemoryContext() {
    assert(!s_usingMpMemory);
    assert(ExecutorContext::getExecutorContext() == s_mpEngine.context);
    ExecutorContext::assignThreadLocals(s_mpEngine);
    s_usingMpMemory = true;
}

void SynchronizedThreadLock::reassumeLowestSiteContext() {
    ExecutorContext::assignThreadLocals(s_enginesByPartitionId.begin()->second);
}

void SynchronizedThreadLock::reassumeLocalSiteContext() {
    s_usingMpMemory = false;
    ExecutorContext::assignThreadLocals(s_enginesByPartitionId.find(ThreadLocalPool::getThreadPartitionId())->second);
}

bool SynchronizedThreadLock::isLowestSiteContext() {
    return ExecutorContext::getExecutorContext() == s_enginesByPartitionId.begin()->second.context;
}

ExecuteWithMpMemory::ExecuteWithMpMemory() {
    VOLT_TRACE("Entering UseMPmemory");
    SynchronizedThreadLock::assumeMpMemoryContext();
}
ExecuteWithMpMemory::~ExecuteWithMpMemory() {
    VOLT_TRACE("Exiting UseMPmemory");
    SynchronizedThreadLock::reassumeLocalSiteContext();
}

ConditionalExecuteWithMpMemory::ConditionalExecuteWithMpMemory(bool needMpMemory) : m_usingMpMemory(needMpMemory) {
    if (m_usingMpMemory) {
        VOLT_TRACE("Entering UseMPmemory");
        SynchronizedThreadLock::assumeMpMemoryContext();
    }
}

ConditionalExecuteWithMpMemory::~ConditionalExecuteWithMpMemory() {
    if (m_usingMpMemory) {
        VOLT_TRACE("Exiting UseMPmemory");
        SynchronizedThreadLock::reassumeLocalSiteContext();
    }
}

}
