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

#pragma once

#include "Topend.h"
#include "common/LargeTempTableBlockCache.h"
#include "common/UndoQuantum.h"
#include "common/valuevector.h"
#include "common/subquerycontext.h"
#include "common/ValuePeeker.hpp"
#include "common/UniqueId.hpp"
#include "execution/ExecutorVector.h"
#include "execution/VoltDBEngine.h"
#include "common/ThreadLocalPool.h"

#include <vector>
#include <stack>
#include <map>

namespace voltdb {

extern const int64_t VOLT_EPOCH;
extern const int64_t VOLT_EPOCH_IN_MILLIS;

// how many initial tuples to scan before calling into java
const int64_t LONG_OP_THRESHOLD = 10000;

class AbstractExecutor;
class AbstractDRTupleStream;
class VoltDBEngine;
class UndoQuantum;
struct EngineLocals;

// Some unit tests require the re-initialization of the
// SynchronizedThreadLock globals. We do this here so that
// the next time the first executor gets created we will
// (re)initialize any necessary global state.
void globalDestroyOncePerProcess();

struct ProgressStats {
    int64_t TuplesProcessedInBatch = 0;
    int64_t TuplesProcessedInFragment = 0;
    int64_t TuplesProcessedSinceReport = 0;
    int64_t TupleReportThreshold = LONG_OP_THRESHOLD;
    PlanNodeType LastAccessedPlanNodeType = PlanNodeType::Invalid;
    void resetForNewBatch() {
        TuplesProcessedInBatch = TuplesProcessedInFragment = TuplesProcessedSinceReport = 0;
    }
    void rollUpForPlanFragment() {
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
    ExecutorContext(int64_t siteId, CatalogId partitionId, UndoQuantum *undoQuantum,
            Topend* topend, Pool* tempStringPool, VoltDBEngine* engine,
            std::string const& hostname, CatalogId hostId, AbstractDRTupleStream *drTupleStream,
            AbstractDRTupleStream *drReplicatedStream, CatalogId drClusterId);

    ~ExecutorContext();

    // It is the thread-hopping VoltDBEngine's responsibility to re-establish the EC for each new thread it runs on.
    void bindToThread();
    static void assignThreadLocals(const EngineLocals& mapping);
    static void resetStateForTest();

    // not always known at initial construction
    void setPartitionId(CatalogId partitionId) {
        m_partitionId = partitionId;
    }

    int32_t getPartitionId() const {
        return m_partitionId;
    }

    // helper to configure the context for a new jni call
    void setupForPlanFragments(UndoQuantum *undoQuantum,
            int64_t txnId, int64_t spHandle, int64_t lastCommittedSpHandle,
            int64_t uniqueId, bool traceOn) {
        m_undoQuantum = undoQuantum;
        m_spHandle = spHandle;
        m_txnId = txnId;
        m_lastCommittedSpHandle = lastCommittedSpHandle;
        m_uniqueId = uniqueId;
        m_currentDRTimestamp = createDRTimestampHiddenValue(static_cast<int64_t>(m_drClusterId), m_uniqueId);
        m_traceOn = traceOn;
        // reset stats for each plan
        m_progressStats.resetForNewBatch();
    }

    // data available via tick()
    void setupForTick(int64_t lastCommittedSpHandle) {
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

    void setupForExecutors(std::map<int, std::vector<AbstractExecutor*>>* executorsMap) {
        vassert(executorsMap != NULL);
        m_executorsMap = executorsMap;
        vassert(m_subqueryContextMap.empty());
        vassert(m_commonTableMap.empty());
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

    UndoQuantum *getCurrentUndoQuantum() const {
        return m_undoQuantum;
    }

    static VoltDBEngine* getEngine() {
        auto ctx = getExecutorContext();
        return ctx == nullptr ? nullptr : ctx->m_engine;
    }

    static UndoQuantum *currentUndoQuantum() {
        return getExecutorContext()->m_undoQuantum;
    }

    /*
     * This returns the topend for the currently running
     * thread.  This may be a thread working on behalf of
     * some other thread.  Calls to the jni have to use
     * this function to get the topend.
     */
    static Topend* getPhysicalTopend();

    /** Current or most recent sp handle */
    int64_t currentSpHandle() const {
        return m_spHandle;
    }

    /** Current or most recent txnid, may go backwards due to multiparts */
    int64_t currentTxnId() const {
        return m_txnId;
    }

    /** Timestamp from unique id for this transaction */
    int64_t currentUniqueId() const {
        return m_uniqueId;
    }

    /** DR cluster id for the local cluster */
    int32_t drClusterId() const {
        return m_drClusterId;
    }

    /** Last committed transaction known to this EE */
    int64_t lastCommittedSpHandle() const {
        return m_lastCommittedSpHandle;
    }

    /** DR timestamp field value for this transaction */
    int64_t currentDRTimestamp() const {
        return m_currentDRTimestamp;
    }

    bool isTraceOn() const {
        return m_traceOn;
    }

    bool externalStreamsEnabled() const {
        return m_externalStreamsEnabled;
    }

    void disableExternalStreams() {
        m_externalStreamsEnabled = false;
    }

    VoltDBEngine* getContextEngine() const {
        return m_engine;
    }

    /** Executor List for a given sub statement id */
    const std::vector<AbstractExecutor*>& getExecutors(int subqueryId) const {
        vassert(m_executorsMap->find(subqueryId) != m_executorsMap->end());
        return m_executorsMap->find(subqueryId)->second;
    }

    /** Return pointer to a subquery context or NULL */
    SubqueryContext* getSubqueryContext(int subqueryId) {
        auto const it = m_subqueryContextMap.find(subqueryId);
        if (it != m_subqueryContextMap.cend()) {
            return &it->second;
        } else {
            return nullptr;
        }
    }

    /** Set a new subquery context for the statement id. */
    SubqueryContext* setSubqueryContext(int subqueryId, const std::vector<NValue>& lastParams) {
        auto const& r = m_subqueryContextMap.emplace(subqueryId, SubqueryContext(lastParams));
        vassert(r.second);
        return &r.first->second;
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
    UniqueTempTableResult executeExecutors(
            const std::vector<AbstractExecutor*>& executorList, int subqueryId = 0);

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

    AbstractDRTupleStream* drStream() const {
        return m_drStream;
    }

    AbstractDRTupleStream* drReplicatedStream() const {
        return m_drReplicatedStream;
    }

    /**
     * Get the executor context of the site which is
     * currently the logically executing thread.
     *
     * @return The executor context of the logical site.
     */
    static ExecutorContext* getExecutorContext();

    /**
     * Get the top end of the site which is currently
     * working.  This is generally the same as getExecutorContext()->getTopend().
     * But sometimes, when updating a shared replicated table, the
     * site doing the updating does work on behalf of all other
     * sites.  In this case the other sites, not the sites doing
     * the work, are acting as free riders.
     *
     * @return The ExecutorContext of the working site.
     */
    static Topend *getThreadTopend();

    static Pool* getTempStringPool() {
        ExecutorContext* singleton = getExecutorContext();
        vassert(singleton != NULL);
        vassert(singleton->m_tempStringPool != NULL);
        return singleton->m_tempStringPool;
    }

    bool allOutputTempTablesAreEmpty() const;

    bool checkTransactionForDR();

    void setUsedParameterCount(int usedParamcnt) {
        m_usedParamcnt = usedParamcnt;
    }
    int getUsedParameterCount() const {
        return m_usedParamcnt;
    }
    NValueArray& getParameterContainer() {
        return m_staticParams;
    }
    const NValueArray& getParameterContainer() const {
        return m_staticParams;
    }

    void pushNewModifiedTupleCounter() {
        m_tuplesModifiedStack.push(0);
    }
    void popModifiedTupleCounter() {
        m_tuplesModifiedStack.pop();
    }
    const int64_t getModifiedTupleCount() const {
        vassert(m_tuplesModifiedStack.size() > 0);
        return m_tuplesModifiedStack.top();
    }
    const size_t getModifiedTupleStackSize() const {
        return m_tuplesModifiedStack.size();
    }

    /** DML executors call this to indicate how many tuples
         * have been modified */
    void addToTuplesModified(int64_t amount) {
        vassert(m_tuplesModifiedStack.size() > 0);
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
    int64_t pullTuplesRemainingUntilProgressReport(PlanNodeType planNodeType) {
        m_progressStats.LastAccessedPlanNodeType = planNodeType;
        return m_progressStats.TupleReportThreshold - m_progressStats.TuplesProcessedSinceReport;
    }

    /**
     * Called periodically during a long-running operation to see
     * if we need to report a long-running fragment.
     */
    int64_t pushTuplesProcessedForProgressMonitoring(const TempTableLimits* limits,
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

        m_progressStats.LastAccessedPlanNodeType = PlanNodeType::Invalid;
    }

    /**
     * Get the common table with the specified name.
     * If the table does not yet exist, run the specified statement
     * to generate it.
     */
    AbstractTempTable* getCommonTable(const std::string& tableName, int cteStmtId);

    /**
     * Set the common table map entry for the specified name
     * to point to the specified table.
     */
    void setCommonTable(const std::string& tableName, AbstractTempTable* table) {
        m_commonTableMap[tableName] = table;
    }

    /**
     * Call into the topend with information about how executing a plan fragment is going.
     */
    void reportProgressToTopend(const TempTableLimits* limits);

    LargeTempTableBlockCache& lttBlockCache() {
        return m_lttBlockCache;
    }

  private:
    /**
     * This holds the top end for this executor context.  Don't
     * use this, however.  Use the result of calling getPhysicalTopend().
     * This is because sometimes this ExecutorContext is used by some
     * other site when this site is a free rider.  In this case we will
     * always, always, always want to use the top end of the site
     * actually doing the work.
     */
    Topend *m_topend;
    Pool *m_tempStringPool;
    UndoQuantum *m_undoQuantum;

    /** reused parameter container. */
    NValueArray m_staticParams{MAX_PARAM_COUNT};
    /** TODO : should be passed as execute() parameter..*/
    int m_usedParamcnt = 0;

    /** Counts tuples modified by a plan fragments.  Top of stack is the
     * most deeply nested executing plan fragment.
     */
    std::stack<int64_t> m_tuplesModifiedStack{};

    // Executor stack map. The key is the statement id (0 means the main/parent statement)
    // The value is the pointer to the executor stack for that statement
    std::map<int, std::vector<AbstractExecutor*>>* m_executorsMap = nullptr;
    std::map<std::string, AbstractTempTable*> m_commonTableMap;
    std::map<int, SubqueryContext> m_subqueryContextMap{};

    AbstractDRTupleStream *m_drStream;
    AbstractDRTupleStream *m_drReplicatedStream;
    VoltDBEngine *m_engine;
    int64_t m_txnId = 0;
    int64_t m_spHandle = 0;
    int64_t m_uniqueId = 0;
    int64_t m_currentTxnTimestamp;
    int64_t m_currentDRTimestamp = 0;
    LargeTempTableBlockCache m_lttBlockCache;
    bool m_traceOn = false;
    // used by elastic shrink once all data has been migrated away
    // from this partition. The site will continue to participate in MP txns
    // until the site is removed fully from the system, but we want to disable
    // all streaming (export, DR) because the sites are done at this point.
    bool m_externalStreamsEnabled = true;

  public:
    int64_t m_lastCommittedSpHandle = 0;
    int64_t m_siteId;
    CatalogId m_partitionId;
    std::string m_hostname;
    CatalogId m_hostId;
    CatalogId m_drClusterId;
    ProgressStats m_progressStats{};
};

struct EngineLocals : public PoolLocals {
    inline EngineLocals() = default;
    inline explicit EngineLocals(bool dummyEntry) : PoolLocals(dummyEntry), context(NULL) {}
    inline explicit EngineLocals(ExecutorContext* ctxt) : PoolLocals(), context(ctxt) {}
    inline EngineLocals(const EngineLocals& src) : PoolLocals(src), context(src.context) {}

    inline EngineLocals& operator=(EngineLocals const& rhs) {
        PoolLocals::operator=(rhs);
        context = rhs.context;
        return *this;
    }

    ExecutorContext* context = ExecutorContext::getExecutorContext();
};
}

