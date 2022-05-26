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
#include "common/executorcontext.hpp"
#include "common/SynchronizedThreadLock.h"

#include "executors/abstractexecutor.h"
#include "executors/insertexecutor.h"
#include "storage/AbstractDRTupleStream.h"
#include "storage/DRTupleStreamUndoAction.h"
#include "storage/persistenttable.h"
#include "plannodes/insertnode.h"
#include "debuglog.h"

#ifdef LINUX
#include <malloc.h>
#endif // LINUX

using namespace std;

namespace voltdb {

namespace {
/**
 * This is the pthread_specific key for the ExecutorContext
 * of the logically executing site.  See ExecutorContext::getExecutorContext
 * and ExecutorContext::getThreadExecutorContext.
 */
pthread_key_t logical_executor_context_static_key;
/**
 * This is the pthread_specific key for the Topend
 * of the site actually executing.  See ExecutorContext::getExecutorContext
 * and ExecutorContext::getThreadExecutorContext.
 */
pthread_key_t physical_topend_static_key;
pthread_once_t static_keyOnce = PTHREAD_ONCE_INIT;

/**
 * This function will initiate global settings and create thread key once per process.
 * */
void globalInitOrCreateOncePerProcess() {
#ifdef LINUX
    // We ran into an issue where memory wasn't being returned to the
    // operating system (and thus reducing RSS) when freeing. See
    // ENG-891 for some info. It seems that some code we use somewhere
    // (maybe JVM, but who knows) calls mallopt and changes some of
    // the tuning parameters. At the risk of making that software
    // angry, the following code resets the tunable parameters to
    // their default values.

    // Note: The parameters and default values come from looking at
    // the glibc 2.5 source, which is the version that ships
    // with redhat/centos 5. The code seems to also be effective on
    // newer versions of glibc (tested againsts 2.12.1).

    mallopt(M_MXFAST, 128);                 // DEFAULT_MXFAST
    // note that DEFAULT_MXFAST was increased to 128 for 64-bit systems
    // sometime between glibc 2.5 and glibc 2.12.1
    mallopt(M_TRIM_THRESHOLD, 128 * 1024);  // DEFAULT_TRIM_THRESHOLD
    mallopt(M_TOP_PAD, 0);                  // DEFAULT_TOP_PAD
    mallopt(M_MMAP_THRESHOLD, 128 * 1024);  // DEFAULT_MMAP_THRESHOLD
    mallopt(M_MMAP_MAX, 65536);             // DEFAULT_MMAP_MAX
    mallopt(M_CHECK_ACTION, 3);             // DEFAULT_CHECK_ACTION
#endif // LINUX
    // Be explicit about running in the standard C locale for now.
    std::locale::global(std::locale("C"));
    setenv("TZ", "UTC", 0); // set timezone as "UTC" in EE level

    (void) pthread_key_create(&logical_executor_context_static_key, NULL);
    (void) pthread_key_create(&physical_topend_static_key, NULL);
    SynchronizedThreadLock::create();
}

}

void globalDestroyOncePerProcess() {
    SynchronizedThreadLock::destroy();
    pthread_key_delete(logical_executor_context_static_key);
    pthread_key_delete(physical_topend_static_key);
    static_keyOnce = PTHREAD_ONCE_INIT;
}

ExecutorContext::ExecutorContext(int64_t siteId, CatalogId partitionId, UndoQuantum *undoQuantum,
        Topend* topend, Pool* tempStringPool, VoltDBEngine* engine, std::string const& hostname,
        CatalogId hostId, AbstractDRTupleStream *drStream, AbstractDRTupleStream *drReplicatedStream,
        CatalogId drClusterId) : m_topend(topend), m_tempStringPool(tempStringPool),
    m_undoQuantum(undoQuantum), m_drStream(drStream),
    m_drReplicatedStream(drReplicatedStream),
    m_engine(engine),
    m_lttBlockCache(topend, engine ? engine->tempTableMemoryLimit() : 50*1024*1024, siteId), // engine may be null in unit tests
    m_siteId(siteId), m_partitionId(partitionId), m_hostname(hostname),
    m_hostId(hostId), m_drClusterId(drClusterId) {
    (void)pthread_once(&static_keyOnce, globalInitOrCreateOncePerProcess);
    bindToThread();
}

ExecutorContext::~ExecutorContext() {
    m_lttBlockCache.releaseAllBlocks();

    // currently does not own any of its pointers

    VOLT_DEBUG("De-installing EC(%ld) for partition %d", (long)this, m_partitionId);

    pthread_setspecific(logical_executor_context_static_key, NULL);
    pthread_setspecific(physical_topend_static_key, NULL);
}

void ExecutorContext::assignThreadLocals(const EngineLocals& mapping) {
    pthread_setspecific(logical_executor_context_static_key, const_cast<ExecutorContext*>(mapping.context));
    ThreadLocalPool::assignThreadLocals(mapping);
}

void ExecutorContext::resetStateForTest() {
    pthread_setspecific(logical_executor_context_static_key, NULL);
    pthread_setspecific(physical_topend_static_key, NULL);
    ThreadLocalPool::resetStateForTest();
}

void ExecutorContext::bindToThread() {
    pthread_setspecific(logical_executor_context_static_key, this);
    // At this point the logical and physical sites must be the
    // same.  So the two top ends are identical.
    pthread_setspecific(physical_topend_static_key, m_topend);
    VOLT_DEBUG("Installing EC(%p) for partition %d", this, m_partitionId);
}

ExecutorContext* ExecutorContext::getExecutorContext() {
    (void)pthread_once(&static_keyOnce, globalInitOrCreateOncePerProcess);
    return static_cast<ExecutorContext*>(pthread_getspecific(logical_executor_context_static_key));
}

Topend* ExecutorContext::getPhysicalTopend() {
    (void)pthread_once(&static_keyOnce, globalInitOrCreateOncePerProcess);
    return static_cast<Topend *>(pthread_getspecific(physical_topend_static_key));
}

UniqueTempTableResult ExecutorContext::executeExecutors(int subqueryId) {
    const std::vector<AbstractExecutor*>& executorList = getExecutors(subqueryId);
    return executeExecutors(executorList, subqueryId);
}

UniqueTempTableResult ExecutorContext::executeExecutors(
      const std::vector<AbstractExecutor*>& executorList, int subqueryId) {
    // Walk through the list and execute each plannode.
    // The query planner guarantees that for a given plannode,
    // all of its children are positioned before it in this list,
    // therefore dependency tracking is not needed here.
    int ctr = 0;
    try {
        for (AbstractExecutor *executor: executorList) {
            vassert(executor);

            if (isTraceOn()) {
                char name[32];
                snprintf(name, 32, "%s", planNodeToString(executor->getPlanNode()->getPlanNodeType()).c_str());
                getPhysicalTopend()->traceLog(true, name, NULL);
            }

            // Call the execute method to actually perform whatever action
            // it is that the node is supposed to do...
            if (!executor->execute(m_staticParams)) {
                if (isTraceOn()) {
                    getPhysicalTopend()->traceLog(false, NULL, NULL);
                }
                InsertExecutor* insertExecutor = dynamic_cast<InsertExecutor*>(executor);
                if (insertExecutor != nullptr && insertExecutor->exceptionMessage() != nullptr) {
                   throw SerializableEEException(insertExecutor->exceptionMessage());
                } else {
                   throw SerializableEEException("Unspecified execution error detected");
                }
            }

            if (isTraceOn()) {
                getPhysicalTopend()->traceLog(false, NULL, NULL);
            }

            ++ctr;
        }
    } catch (const SerializableEEException &e) {
        if (SynchronizedThreadLock::isInSingleThreadMode()) {
            // Assign the correct pool back to this thread
            SynchronizedThreadLock::signalLowestSiteFinished();
        }

        // Clean up any tempTables when the plan finishes abnormally.
        // This needs to be the caller's responsibility for normal returns because
        // the caller may want to first examine the final output table.
        cleanupAllExecutors();
        // Normally, each executor cleans its memory pool as it finishes execution,
        // but in the case of a throw, it may not have had the chance.
        // So, clean up all the memory pools now.
        //TODO: This code singles out inline nodes for cleanup.
        // Is that because the currently active (memory pooling) non-inline
        // executor always cleans itself up before throwing???
        // But if an active executor can be that smart, an active executor with
        // (potential) inline children could also be smart enough to clean up
        // after its inline children, and this post-processing would not be needed.
        for (AbstractExecutor *executor: executorList) {
            vassert(executor);
            AbstractPlanNode * node = executor->getPlanNode();
            std::map<PlanNodeType, AbstractPlanNode*>::iterator it;
            std::map<PlanNodeType, AbstractPlanNode*> inlineNodes = node->getInlinePlanNodes();
            for (it = inlineNodes.begin(); it != inlineNodes.end(); it++ ) {
                AbstractPlanNode *inlineNode = it->second;
                inlineNode->getExecutor()->cleanupMemoryPool();
            }
        }
        if (subqueryId == 0) {
            VOLT_TRACE("The Executor's execution at position '%d' failed", ctr);
        } else {
            VOLT_TRACE("The Executor's execution at position '%d' in subquery %d failed", ctr, subqueryId);
        }
        throw;
    }

    AbstractTempTable *result = executorList.back()->getPlanNode()->getTempOutputTable();
    return UniqueTempTableResult(result);
}

Table* ExecutorContext::getSubqueryOutputTable(int subqueryId) const {
    const std::vector<AbstractExecutor*>& executorList = getExecutors(subqueryId);
    vassert(!executorList.empty());
    return executorList.back()->getPlanNode()->getOutputTable();
}

AbstractTempTable* ExecutorContext::getCommonTable(const std::string& tableName, int cteStmtId) {
    AbstractTempTable* table = NULL;
    auto it = m_commonTableMap.find(tableName);
    if (it == m_commonTableMap.end()) {
        UniqueTempTableResult result = executeExecutors(cteStmtId);
        table = result.release();
        m_commonTableMap.insert(std::make_pair(tableName, table));
    } else {
        table = it->second;
    }

    return table;
}

void ExecutorContext::cleanupAllExecutors() {
    // If something failed before we could even instantiate the plan,
    // there won't even be an executors map.
    if (m_executorsMap != NULL) {
        for(auto& entry : *m_executorsMap) {
            int subqueryId = entry.first;
            cleanupExecutorsForSubquery(subqueryId);
        }
    }

    // Clear any cached results from executed subqueries
    m_subqueryContextMap.clear();
    m_commonTableMap.clear();
}

void ExecutorContext::cleanupExecutorsForSubquery(
        const std::vector<AbstractExecutor*>& executorList) const {
    for (AbstractExecutor *executor: executorList) {
        vassert(executor);
        executor->cleanupTempOutputTable();
    }
}

void ExecutorContext::cleanupExecutorsForSubquery(int subqueryId) const {
    const std::vector<AbstractExecutor*>& executorList = getExecutors(subqueryId);
    cleanupExecutorsForSubquery(executorList);
}

void ExecutorContext::resetExecutionMetadata(ExecutorVector* executorVector) {

    if (m_tuplesModifiedStack.size() != 0) {
        m_tuplesModifiedStack.pop();
    }
    vassert(m_tuplesModifiedStack.size() == 0);

    executorVector->resetLimitStats();
}

void ExecutorContext::reportProgressToTopend(const TempTableLimits *limits) {

    int64_t allocated = limits != NULL ? limits->getAllocated() : -1;
    int64_t peak = limits != NULL ? limits->getPeakMemoryInBytes() : -1;

    //Update stats in java and let java determine if we should cancel this query.
    m_progressStats.TuplesProcessedInFragment += m_progressStats.TuplesProcessedSinceReport;

    int64_t tupleReportThreshold = getPhysicalTopend()->fragmentProgressUpdate(
            m_engine->getCurrentIndexInBatch(), m_progressStats.LastAccessedPlanNodeType,
            m_progressStats.TuplesProcessedInBatch + m_progressStats.TuplesProcessedInFragment,
            allocated, peak);
    m_progressStats.TuplesProcessedSinceReport = 0;

    if (tupleReportThreshold < 0) {
        VOLT_DEBUG("Interrupt query.");
        char buff[100];
        /**
         * NOTE: this is NOT a broken sentence. The complete
         * error message reported will be:
         *
         * VOLTDB ERROR: Transaction Interrupted A SQL query was terminated after 1.00# seconds because it exceeded the query-specific timeout period. The query-specific timeout is currently 1.0 seconds. The default query timeout is currently 10.0 seconds and can be changed in the systemsettings section of the deployment file.
         *
         * See also: tests/sqlcmd/scripts/querytimeout/timeout.err
         */
        snprintf(buff, 100,
                "A SQL query was terminated after %.03f seconds because it exceeded the",
                static_cast<double>(tupleReportThreshold) / -1000.0);
        throw InterruptException(std::string(buff));
    }
    m_progressStats.TupleReportThreshold = tupleReportThreshold;
}

bool ExecutorContext::allOutputTempTablesAreEmpty() const {
    if (m_executorsMap != nullptr) {
        for(auto& entry : *m_executorsMap) {
            for(auto const* executor : entry.second) {
                if (! executor->outputTempTableIsEmpty()) {
                    return false;
                }
            }
        }
    }

    return true;
}

void ExecutorContext::setDrStream(AbstractDRTupleStream *drStream) {
    vassert(m_drStream != NULL);
    vassert(drStream != NULL);
    vassert(m_drStream->m_committedSequenceNumber >= drStream->m_committedSequenceNumber);
    int64_t lastCommittedSpHandle = std::max(m_lastCommittedSpHandle, drStream->m_openTxnId);
    m_drStream->periodicFlush(-1L, lastCommittedSpHandle);
    int64_t oldSeqNum = m_drStream->m_committedSequenceNumber;
    m_drStream = drStream;
    m_drStream->setLastCommittedSequenceNumber(oldSeqNum);
}

void ExecutorContext::setDrReplicatedStream(AbstractDRTupleStream *drReplicatedStream) {
    if (m_drReplicatedStream == NULL || drReplicatedStream == NULL) {
        m_drReplicatedStream = drReplicatedStream;
        return;
    }
    vassert(m_drReplicatedStream->m_committedSequenceNumber >= drReplicatedStream->m_committedSequenceNumber);
    int64_t lastCommittedSpHandle = std::max(m_lastCommittedSpHandle, drReplicatedStream->m_openTxnId);
    m_drReplicatedStream->periodicFlush(-1L, lastCommittedSpHandle);
    int64_t oldSeqNum = m_drReplicatedStream->m_committedSequenceNumber;
    m_drReplicatedStream = drReplicatedStream;
    m_drReplicatedStream->setLastCommittedSequenceNumber(oldSeqNum);
}

/**
 * To open DR stream to start binary logging for a transaction at this level,
 *   1. It needs to be a multipartition transaction.
 *   2. It is NOT a read-only transaction as it generates no data change on any partition.
 *
 * For single partition transactions, DR stream's binary logging is handled as is
 * at persistenttable level.
 */
bool ExecutorContext::checkTransactionForDR() {
    bool result = false;
    if (UniqueId::isMpUniqueId(m_uniqueId) && m_undoQuantum != NULL) {
        if (m_externalStreamsEnabled && m_drStream && m_drStream->drStreamStarted()) {
            if (m_drStream->transactionChecks(m_spHandle, m_uniqueId)) {
                m_undoQuantum->registerUndoAction(
                        new (*m_undoQuantum) DRTupleStreamUndoAction(m_drStream,
                                m_drStream->m_committedUso, 0));
            }
            result = true;
        }
        if (m_drReplicatedStream && m_drReplicatedStream->drStreamStarted()) {
            if (m_drReplicatedStream->transactionChecks(m_spHandle, m_uniqueId)) {
                m_undoQuantum->registerUndoAction(
                        new (*m_undoQuantum) DRTupleStreamUndoAction(
                                m_drReplicatedStream,
                                m_drReplicatedStream->m_committedUso, 0));
            }
        }
    }
    return result;
}

} // end namespace voltdb
