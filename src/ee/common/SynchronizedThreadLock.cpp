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

namespace voltdb {

// Initialized when executor context is created.
pthread_mutex_t SynchronizedThreadLock::s_sharedEngineMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t SynchronizedThreadLock::s_sharedEngineCondition;
pthread_cond_t SynchronizedThreadLock::s_wakeLowestEngineCondition;
int32_t SynchronizedThreadLock::s_globalTxnStartCountdownLatch = 0;
int32_t SynchronizedThreadLock::s_SITES_PER_HOST = -1;
bool SynchronizedThreadLock::s_inMpContext = false;
SharedEngineLocalsType SynchronizedThreadLock::s_enginesByPartitionId;

void SynchronizedUndoReleaseAction::undo() {
    if (!SynchronizedThreadLock::isInRepTableContext()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
        m_realAction->undo();
        SynchronizedThreadLock::signalLowestSiteFinished();
    } else {
        m_realAction->undo();
    }
}

void SynchronizedUndoReleaseAction::release() {
    if (!SynchronizedThreadLock::isInRepTableContext()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
        m_realAction->release();
        SynchronizedThreadLock::signalLowestSiteFinished();
    } else {
        m_realAction->release();
    }
}

void SynchronizedUndoOnlyAction::undo() {
    if (!SynchronizedThreadLock::isInRepTableContext()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
        m_realAction->undo();
        SynchronizedThreadLock::signalLowestSiteFinished();
    } else {
        m_realAction->undo();
    }
}

void SynchronizedDummyUndoReleaseAction::undo() {
    if (!SynchronizedThreadLock::isInRepTableContext()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(false);
    }
}

void SynchronizedDummyUndoReleaseAction::release() {
    if (!SynchronizedThreadLock::isInRepTableContext()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(false);
    }
}

void SynchronizedDummyUndoOnlyAction::undo() {
    if (!SynchronizedThreadLock::isInRepTableContext()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(false);
    }
}


void SynchronizedUndoQuantumReleaseInterest::notifyQuantumRelease() {
    if (!SynchronizedThreadLock::isInRepTableContext()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
        m_realInterest->notifyQuantumRelease();
        SynchronizedThreadLock::signalLowestSiteFinished();
    } else {
        m_realInterest->notifyQuantumRelease();
    }
};

void SynchronizedDummyUndoQuantumReleaseInterest::notifyQuantumRelease() {
    if (!SynchronizedThreadLock::isInRepTableContext()) {
        SynchronizedThreadLock::countDownGlobalTxnStartCount(false);
    }
}

void SynchronizedThreadLock::create() {
    assert(s_SITES_PER_HOST == -1);
    s_SITES_PER_HOST = 0;
    pthread_mutex_init(&s_sharedEngineMutex, NULL);
    pthread_cond_init(&s_sharedEngineCondition, 0);
    pthread_cond_init(&s_wakeLowestEngineCondition, 0);
}

void SynchronizedThreadLock::destroy() {
    pthread_cond_destroy(&s_sharedEngineCondition);
    pthread_cond_destroy(&s_wakeLowestEngineCondition);
    pthread_mutex_destroy(&s_sharedEngineMutex);
}

void SynchronizedThreadLock::init(int32_t sitesPerHost, EngineLocals& newEngineLocals) {
    if (s_SITES_PER_HOST == 0) {
        s_SITES_PER_HOST = sitesPerHost;
        s_globalTxnStartCountdownLatch = s_SITES_PER_HOST;
    }
    s_enginesByPartitionId[newEngineLocals.partitionId] = newEngineLocals;
}

bool SynchronizedThreadLock::countDownGlobalTxnStartCount(bool lowestSite) {
    assert(s_globalTxnStartCountdownLatch > 0);
    if (lowestSite) {
        pthread_mutex_lock(&s_sharedEngineMutex);
        if (--s_globalTxnStartCountdownLatch != 0) {
            pthread_cond_wait(&s_wakeLowestEngineCondition, &s_sharedEngineMutex);
        }
        pthread_mutex_unlock(&s_sharedEngineMutex);
        VOLT_DEBUG("Switching context to MP partition on thread %lu", pthread_self());
        s_inMpContext = true;
        return true;
    }
    else {
        VOLT_DEBUG("Waiting for MP partition work to complete on thread %lu", pthread_self());
        pthread_mutex_lock(&s_sharedEngineMutex);
        if (--s_globalTxnStartCountdownLatch == 0) {
            pthread_cond_broadcast(&s_wakeLowestEngineCondition);
        }
        pthread_cond_wait(&s_sharedEngineCondition, &s_sharedEngineMutex);
        pthread_mutex_unlock(&s_sharedEngineMutex);
        assert(!s_inMpContext);
        VOLT_DEBUG("Other SP partition thread released on thread %lu", pthread_self());
        return false;
    }
}

void SynchronizedThreadLock::signalLowestSiteFinished() {
    pthread_mutex_lock(&s_sharedEngineMutex);
    s_globalTxnStartCountdownLatch = s_SITES_PER_HOST;
    VOLT_DEBUG("Restore context to lowest SP partition on thread %lu", pthread_self());
    s_inMpContext = false;
    pthread_cond_broadcast(&s_sharedEngineCondition);
    pthread_mutex_unlock(&s_sharedEngineMutex);
}

void SynchronizedThreadLock::lockReplicatedResource() {
    pthread_mutex_lock(&s_sharedEngineMutex);
    VOLT_DEBUG("Grabbing replicated resource lock on thread %lu", pthread_self());
    assert(!s_inMpContext);
}


void SynchronizedThreadLock::addUndoAction(bool synchronized, UndoQuantum *uq, UndoReleaseAction* action,
        PersistentTable *table) {
    if (synchronized) {
        // For shared replicated table, in the same host site with lowest id
        // will create the actual undo action, other sites register a dummy
        // undo action as placeholder
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

void SynchronizedThreadLock::unlockReplicatedResource() {
    VOLT_DEBUG("Releasing replicated resource lock on thread %lu", pthread_self());
    pthread_mutex_unlock(&s_sharedEngineMutex);
}

bool SynchronizedThreadLock::isInRepTableContext() {
    return s_inMpContext;
}

}
