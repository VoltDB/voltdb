/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#ifndef _EXECUTORCONTEXT_HPP_
#define _EXECUTORCONTEXT_HPP_

#include "Topend.h"
#include "common/UndoQuantum.h"

namespace voltdb {

class DRTupleStream;
class VoltDBEngine;

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
                    VoltDBEngine* engine,
                    std::string hostname,
                    CatalogId hostId,
                    DRTupleStream *drTupleStream);

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
                               int64_t spHandle,
                               int64_t lastCommittedSpHandle,
                               int64_t uniqueId)
    {
        m_undoQuantum = undoQuantum;
        m_spHandle = spHandle;
        m_txnId = txnId;
        m_lastCommittedSpHandle = lastCommittedSpHandle;
        m_currentTxnTimestamp = (m_uniqueId >> 23) + m_epoch;
        m_uniqueId = uniqueId;
    }

    // data available via tick()
    void setupForTick(int64_t lastCommittedSpHandle)
    {
        m_lastCommittedSpHandle = lastCommittedSpHandle;
        m_spHandle = std::max(m_spHandle, lastCommittedSpHandle);
    }

    // data available via quiesce()
    void setupForQuiesce(int64_t lastCommittedSpHandle) {
        m_lastCommittedSpHandle = lastCommittedSpHandle;
        m_spHandle = std::max(lastCommittedSpHandle, m_spHandle);
    }

    // for test (VoltDBEngine::getExecutorContext())
    void setupForPlanFragments(UndoQuantum *undoQuantum) {
        m_undoQuantum = undoQuantum;
    }

    UndoQuantum *getCurrentUndoQuantum() {
        return m_undoQuantum;
    }

    static VoltDBEngine* getEngine() {
        return getExecutorContext()->m_engine;
    }

    static UndoQuantum *currentUndoQuantum() {
        return getExecutorContext()->m_undoQuantum;
    }

    Topend* getTopend() {
        return m_topEnd;
    }

    /** Current or most recent sp handle */
    int64_t currentSpHandle() {
        return m_spHandle;
    }

    /** Current or most recent txnid, may go backwards due to multiparts */
    int64_t currentTxnId() {
        return m_txnId;
    }

    /** Timestamp from unique id for this transaction */
    int64_t currentUniqueId() {
        return m_uniqueId;
    }

    /** Timestamp from unique id for this transaction */
    int64_t currentTxnTimestamp() {
        return m_currentTxnTimestamp;
    }

    /** Last committed transaction known to this EE */
    int64_t lastCommittedSpHandle() {
        return m_lastCommittedSpHandle;
    }

    DRTupleStream* drStream() {
        return m_drStream;
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
    DRTupleStream *m_drStream;
    VoltDBEngine *m_engine;
    int64_t m_txnId;
    int64_t m_spHandle;
    int64_t m_uniqueId;
    int64_t m_currentTxnTimestamp;
  public:
    int64_t m_lastCommittedSpHandle;
    int64_t m_siteId;
    CatalogId m_partitionId;
    std::string m_hostname;
    CatalogId m_hostId;

    /** local epoch for voltdb, somtime around 2008, pulled from catalog */
    int64_t m_epoch;
};

}

#endif
