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

#ifndef _EXECUTORCONTEXT_HPP_
#define _EXECUTORCONTEXT_HPP_

#include "Topend.h"
#include "common/UndoQuantum.h"
#include "common/valuevector.h"
#include "common/subquerycontext.h"
#include "common/ValuePeeker.hpp"
#include "common/UniqueId.hpp"
#include "execution/ExecutorVector.h"
#include "execution/VoltDBEngine.h"

#include <vector>
#include <stack>
#include <map>
#include <memory>

namespace voltdb {

extern const int64_t VOLT_EPOCH;
extern const int64_t VOLT_EPOCH_IN_MILLIS;

// how many initial tuples to scan before calling into java
const int64_t LONG_OP_THRESHOLD = 10000;

class AbstractExecutor;
class AbstractDRTupleStream;
class VoltDBEngine;

class TempTable;

struct ProgressStats {
    int64_t TuplesProcessedInBatch;
    int64_t TuplesProcessedInFragment;
    int64_t TuplesProcessedSinceReport;
    int64_t TupleReportThreshold;
    PlanNodeType LastAccessedPlanNodeType;

    ProgressStats() {
        TuplesProcessedInBatch = TuplesProcessedInFragment = TuplesProcessedSinceReport = 0;
        TupleReportThreshold = LONG_OP_THRESHOLD;
        LastAccessedPlanNodeType = PLAN_NODE_TYPE_INVALID;
    }

    inline void resetForNewBatch() {
        TuplesProcessedInBatch = TuplesProcessedInFragment = TuplesProcessedSinceReport = 0;
    }

    inline void rollUpForPlanFragment() {
        TuplesProcessedInBatch += TuplesProcessedInFragment;
        TuplesProcessedInFragment = 0;
        TuplesProcessedSinceReport = 0;
    }
};

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
    ExecutorContext(int64_t siteId,
                    CatalogId partitionId,
                    UndoQuantum *undoQuantum,
                    Topend* topend,
                    Pool* tempStringPool,
                    VoltDBEngine* engine,
                    std::string hostname,
                    CatalogId hostId,
                    AbstractDRTupleStream *drTupleStream,
                    AbstractDRTupleStream *drReplicatedStream,
                    CatalogId drClusterId);

    ~ExecutorContext();

    // It is the thread-hopping VoltDBEngine's responsibility to re-establish the EC for each new thread it runs on.
    void bindToThread();

    // not always known at initial construction
    void setPartitionId(CatalogId partitionId) {
        m_partitionId = partitionId;
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
        m_uniqueId = uniqueId;
        m_currentTxnTimestamp = (m_uniqueId >> 23) + VOLT_EPOCH_IN_MILLIS;
        m_currentDRTimestamp = createDRTimestampHiddenValue(static_cast<int64_t>(m_drClusterId), m_uniqueId);
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

    // Used originally for test. Now also used to NULL
    // out the UndoQuantum when it is released to make it possible
    // to check if there currently exists an active undo quantum
    // so that things that should only execute after the currently running
    // transaction has committed can assert on that.
    void setupForPlanFragments(UndoQuantum *undoQuantum) {
        m_undoQuantum = undoQuantum;
    }

    void setupForExecutors(std::map<int, std::vector<AbstractExecutor*>* >* executorsMap) {
        assert(executorsMap != NULL);
        m_executorsMap = executorsMap;
        assert(m_subqueryContextMap.empty());
    }

    static int64_t createDRTimestampHiddenValue(int64_t clusterId, int64_t uniqueId) {
        return (clusterId << 49) | (uniqueId >> 14);
    }

    static int64_t getDRTimestampFromHiddenNValue(const NValue &value) {
        int64_t hiddenValue = ValuePeeker::peekAsBigInt(value);
        return UniqueId::tsCounterSinceUnixEpoch(hiddenValue & UniqueId::TIMESTAMP_PLUS_COUNTER_MAX_VALUE);
    }

    static int8_t getClusterIdFromHiddenNValue(const NValue &value) {
        int64_t hiddenValue = ValuePeeker::peekAsBigInt(value);
        return static_cast<int8_t>(hiddenValue >> 49);
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
        return m_topend;
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

    /** DR cluster id for the local cluster */
    int32_t drClusterId() {
        return m_drClusterId;
    }

    /** Last committed transaction known to this EE */
    int64_t lastCommittedSpHandle() {
        return m_lastCommittedSpHandle;
    }

    /** DR timestamp field value for this transaction */
    int64_t currentDRTimestamp() {
        return m_currentDRTimestamp;
    }

    /** Executor List for a given sub statement id */
    const std::vector<AbstractExecutor*>& getExecutors(int subqueryId) const
    {
        assert(m_executorsMap->find(subqueryId) != m_executorsMap->end());
        return *m_executorsMap->find(subqueryId)->second;
    }

    /** Return pointer to a subquery context or NULL */
    SubqueryContext* getSubqueryContext(int subqueryId)
    {
        std::map<int, SubqueryContext>::iterator it = m_subqueryContextMap.find(subqueryId);
        if (it != m_subqueryContextMap.end()) {
            return &(it->second);
        } else {
            return NULL;
        }
    }

    /** Set a new subquery context for the statement id. */
    SubqueryContext* setSubqueryContext(int subqueryId, const std::vector<NValue>& lastParams)
    {
        SubqueryContext fromCopy(lastParams);
#ifdef DEBUG
        std::pair<std::map<int, SubqueryContext>::iterator, bool> result =
#endif
            m_subqueryContextMap.insert(std::make_pair(subqueryId, fromCopy));
        assert(result.second);
        return &(m_subqueryContextMap.find(subqueryId)->second);
    }

    /**
     * Execute all the executors in the given vector.
     *
     * This method will clean up intermediate temporary results, and
     * return the result table of the last executor.
     *
     * The class UniqueTempTableResult is a smart pointer-like object
     * that will delete the rows of the temp table when it goes out of
     * scope.
     *
     * In absence of subqueries, which cache their results for
     * performance, this method takes care of all cleanup
     * aotomatically.
     */
    UniqueTempTableResult executeExecutors(const std::vector<AbstractExecutor*>& executorList,
                                           int subqueryId = 0);

    /**
     * Similar to above method.  Execute the executors associated with
     * the given subquery ID, as defined in m_executorsMap.
     */
    UniqueTempTableResult executeExecutors(int subqueryId);

    /**
     * Return the result produced by the given subquery.
     */
    Table* getSubqueryOutputTable(int subqueryId) const;

    /**
     * Cleanup all the executors in m_executorsMap (includes top-level
     * enclosing fragments and any subqueries), and delete any tuples
     * in temp tables used by the executors.
     */
    void cleanupAllExecutors();

    /**
     * Clean up the executors in the given list.
     */
    void cleanupExecutorsForSubquery(const std::vector<AbstractExecutor*>& executorList) const;

    /**
     * Clean up the executors for the given subquery, as contained in m_executorsMap.
     */
    void cleanupExecutorsForSubquery(int subqueryId) const;

    void resetExecutionMetadata(ExecutorVector* executorVector);

    void setDrStream(AbstractDRTupleStream *drStream);
    void setDrReplicatedStream(AbstractDRTupleStream *drReplicatedStream);

    AbstractDRTupleStream* drStream() {
        return m_drStream;
    }

    AbstractDRTupleStream* drReplicatedStream() {
        return m_drReplicatedStream;
    }

    static ExecutorContext* getExecutorContext();

    static Pool* getTempStringPool() {
        ExecutorContext* singleton = getExecutorContext();
        assert(singleton != NULL);
        assert(singleton->m_tempStringPool != NULL);
        return singleton->m_tempStringPool;
    }

    bool allOutputTempTablesAreEmpty() const;

    void checkTransactionForDR();

    void setUsedParameterCount(int usedParamcnt) { m_usedParamcnt = usedParamcnt; }
    int getUsedParameterCount() const { return m_usedParamcnt; }
    NValueArray& getParameterContainer() { return m_staticParams; }
    const NValueArray& getParameterContainer() const { return m_staticParams; }

    void pushNewModifiedTupleCounter() { m_tuplesModifiedStack.push(0); }
    void popModifiedTupleCounter() { m_tuplesModifiedStack.pop(); }
    const int64_t getModifiedTupleCount() const {
        assert(m_tuplesModifiedStack.size() > 0);
        return m_tuplesModifiedStack.top();
    }
    const size_t getModifiedTupleStackSize() const { return m_tuplesModifiedStack.size(); }

    /** DML executors call this to indicate how many tuples
         * have been modified */
    void addToTuplesModified(int64_t amount) {
        assert(m_tuplesModifiedStack.size() > 0);
        m_tuplesModifiedStack.top() += amount;
    }

    /**
     * Called just before a potentially long-running operation
     * begins execution.
     *
     * Track total tuples accessed for this query.  Set up
     * statistics for long running operations thru m_engine if
     * total tuples accessed passes the threshold.
     */
    inline int64_t pullTuplesRemainingUntilProgressReport(PlanNodeType planNodeType) {
        m_progressStats.LastAccessedPlanNodeType = planNodeType;
        return m_progressStats.TupleReportThreshold - m_progressStats.TuplesProcessedSinceReport;
    }

    /**
     * Called periodically during a long-running operation to see
     * if we need to report a long-running fragment.
     */
    inline int64_t pushTuplesProcessedForProgressMonitoring(const TempTableLimits* limits,
                                                            int64_t tuplesProcessed) {
        m_progressStats.TuplesProcessedSinceReport += tuplesProcessed;
        if (m_progressStats.TuplesProcessedSinceReport >= m_progressStats.TupleReportThreshold) {
            reportProgressToTopend(limits);
        }
        return m_progressStats.TupleReportThreshold; // size of next batch
    }

    /**
     * Called when a long-running operation completes.
     */
    inline void pushFinalTuplesProcessedForProgressMonitoring(const TempTableLimits* limits,
                                                              int64_t tuplesProcessed) {
        try {
            pushTuplesProcessedForProgressMonitoring(limits, tuplesProcessed);
        } catch(const SerializableEEException &e) {
            e.serialize(m_engine->getExceptionOutputSerializer());
        }

        m_progressStats.LastAccessedPlanNodeType = PLAN_NODE_TYPE_INVALID;
    }

    /**
     * Call into the topend with information about how executing a plan fragment is going.
     */
    void reportProgressToTopend(const TempTableLimits* limits);

  private:
    Topend *m_topend;
    Pool *m_tempStringPool;
    UndoQuantum *m_undoQuantum;

    /** reused parameter container. */
    NValueArray m_staticParams;
    /** TODO : should be passed as execute() parameter..*/
    int m_usedParamcnt;

    /** Counts tuples modified by a plan fragments.  Top of stack is the
     * most deeply nested executing plan fragment.
     */
    std::stack<int64_t> m_tuplesModifiedStack;

    // Executor stack map. The key is the statement id (0 means the main/parent statement)
    // The value is the pointer to the executor stack for that statement
    std::map<int, std::vector<AbstractExecutor*>* >* m_executorsMap;
    std::map<int, SubqueryContext> m_subqueryContextMap;

    AbstractDRTupleStream *m_drStream;
    AbstractDRTupleStream *m_drReplicatedStream;
    VoltDBEngine *m_engine;
    int64_t m_txnId;
    int64_t m_spHandle;
    int64_t m_uniqueId;
    int64_t m_currentTxnTimestamp;
    int64_t m_currentDRTimestamp;
  public:
    int64_t m_lastCommittedSpHandle;
    int64_t m_siteId;
    CatalogId m_partitionId;
    std::string m_hostname;
    CatalogId m_hostId;
    CatalogId m_drClusterId;
    ProgressStats m_progressStats;
};

}

#endif
