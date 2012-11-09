/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

#ifndef _EXECUTORCONTEXT_HPP_
#define _EXECUTORCONTEXT_HPP_

#include "Topend.h"
#include "common/UndoQuantum.h"

namespace voltdb {

/*
 * EE site global data required by executors at runtime.
 *
 * This data is factored into common to avoid creating dependencies on
 * execution/VoltDBEngine throughout the storage and executor code.
 * This facilitates easier test case writing and breaks circular
 * dependencies between ee component directories.
 *
 * A better implementation that meets these goals is always welcome if
 * you see a preferable refactoring.
 */
class ExecutorContext {
  public:
    ~ExecutorContext();

    ExecutorContext(int64_t siteId,
                    CatalogId partitionId,
                    UndoQuantum *undoQuantum,
                    Topend* topend,
                    Pool* tempStringPool,
                    bool exportEnabled,
                    std::string hostname,
                    CatalogId hostId);

    // It is the thread-hopping VoltDBEngine's responsibility to re-establish the EC for each new thread it runs on.
    void bindToThread();

    // not always known at initial construction
    void setPartitionId(CatalogId partitionId) {
        m_partitionId = partitionId;
    }

    // not always known at initial construction
    void setEpoch(int64_t epoch) {
        m_epoch = epoch;
    }

    // helper to configure the context for a new jni call
    void setupForPlanFragments(UndoQuantum *undoQuantum,
                               int64_t txnId,
                               int64_t lastCommittedTxnId)
    {
        m_undoQuantum = undoQuantum;
        m_txnId = txnId;
        m_lastCommittedTxnId = lastCommittedTxnId;
    }

    // data available via tick()
    void setupForTick(int64_t lastCommittedTxnId)
    {
        m_lastCommittedTxnId = lastCommittedTxnId;
    }

    // data available via quiesce()
    void setupForQuiesce(int64_t lastCommittedTxnId) {
        m_lastCommittedTxnId = lastCommittedTxnId;
    }

    // for test (VoltDBEngine::getExecutorContext())
    void setupForPlanFragments(UndoQuantum *undoQuantum) {
        m_undoQuantum = undoQuantum;
    }

    UndoQuantum *getCurrentUndoQuantum() {
        return m_undoQuantum;
    }

    static UndoQuantum *currentUndoQuantum() {
        return getExecutorContext()->m_undoQuantum;
    }

    Topend* getTopend() {
        return m_topEnd;
    }

    /** Current or most recently executed transaction id. */
    int64_t currentTxnId() {
        return m_txnId;
    }

    /** Current or most recently executed transaction id. */
    int64_t currentTxnTimestamp() {
        return (m_txnId >> 23) + m_epoch;
    }

    /** Last committed transaction known to this EE */
    int64_t lastCommittedTxnId() {
        return m_lastCommittedTxnId;
    }

    static ExecutorContext* getExecutorContext();

    static Pool* getTempStringPool() {
        ExecutorContext* singleton = getExecutorContext();
        assert(singleton != NULL);
        assert(singleton->m_tempStringPool != NULL);
        return singleton->m_tempStringPool;
    }

  private:
    Topend *m_topEnd;
    Pool *m_tempStringPool;
    UndoQuantum *m_undoQuantum;
    int64_t m_txnId;

  public:
    int64_t m_lastCommittedTxnId;
    int64_t m_siteId;
    CatalogId m_partitionId;
    std::string m_hostname;
    CatalogId m_hostId;
    bool m_exportEnabled;

    /** local epoch for voltdb, somtime around 2008, pulled from catalog */
    int64_t m_epoch;
};

}

#endif
