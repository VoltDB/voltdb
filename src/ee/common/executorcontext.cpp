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
#include "common/executorcontext.hpp"
#include "common/SynchronizedThreadLock.h"

#include "common/debuglog.h"
#include "executors/abstractexecutor.h"
#include "storage/AbstractDRTupleStream.h"
#include "storage/DRTupleStream.h"
#include "storage/DRTupleStreamUndoAction.h"
#include "storage/persistenttable.h"
#include "plannodes/insertnode.h"

#include "boost/foreach.hpp"

#include "expressions/functionexpression.h" // Really for datefunctions and its dependencies.

#include <pthread.h>
#ifdef LINUX
#include <malloc.h>
#endif // LINUX

using namespace std;

namespace voltdb {

SharedEngineLocalsType enginesByPartitionId;
EngineLocals mpEngineLocals;
AbstractExecutor * mpExecutor = NULL;
EngineLocals* ExecutorContext::savedEngineLocals;

static pthread_key_t static_key;
static pthread_once_t static_keyOnce = PTHREAD_ONCE_INIT;

/**
 * This function will initiate global settings and create thread key once per process.
 * */
static void globalInitOrCreateOncePerProcess() {
#ifdef LINUX
    // We ran into an issue where memory wasn't being returned to the
    // operating system (and thus reducing RSS) when freeing. See
    // ENG-891 for some info. It seems that some code we use somewhere
    // (maybe JVM, but who knows) calls mallopt and changes some of
    // the tuning parameters. At the risk of making that software
    // angry, the following code resets the tunable parameters to
    // their default values.

    // Note: The parameters and default values come from looking at
    // the glibc 2.5 source, which I is the version that shipps
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

    (void)pthread_key_create(&static_key, NULL);

    assert(SITES_PER_HOST == -1);
    SITES_PER_HOST = 0;
    pthread_mutex_init(&sharedEngineMutex, NULL);
    pthread_cond_init(&sharedEngineCondition, 0);
}

ExecutorContext::ExecutorContext(int64_t siteId,
                CatalogId partitionId,
                UndoQuantum *undoQuantum,
                Topend* topend,
                Pool* tempStringPool,
                VoltDBEngine* engine,
                std::string hostname,
                CatalogId hostId,
                AbstractDRTupleStream *drStream,
                AbstractDRTupleStream *drReplicatedStream,
                CatalogId drClusterId) :
    m_topend(topend),
    m_tempStringPool(tempStringPool),
    m_undoQuantum(undoQuantum),
    m_staticParams(MAX_PARAM_COUNT),
    m_tuplesModifiedStack(),
    m_executorsMap(),
    m_drStream(drStream),
    m_drReplicatedStream(drReplicatedStream),
    m_engine(engine),
    m_txnId(0),
    m_spHandle(0),
    m_lastCommittedSpHandle(0),
    m_siteId(siteId),
    m_partitionId(partitionId),
    m_hostname(hostname),
    m_hostId(hostId),
    m_drClusterId(drClusterId),
    m_progressStats()
{
    (void)pthread_once(&static_keyOnce, globalInitOrCreateOncePerProcess);
    bindToThread();
}

ExecutorContext::~ExecutorContext() {
    // currently does not own any of its pointers

    // ... or none, now that the one is going away.
    VOLT_DEBUG("De-installing EC(%ld) for partition %d", (long)this, m_partitionId);

    pthread_setspecific(static_key, NULL);
}

/**
 * Switch executor context when updating MP related data structures.
 *
 * Calling this function will switch the executor context from current EngineLocals to the MP EngineLocals,
 * it also changes the associated thread-specific memory pools. The original thread locals are kept in a static
 * variable, it's safe since updating replicated table is always running on a single thread (the lowest site within a node).
 *
 * Must be paired with restoreContext() to restore the original thread-specific variables.
 *
 */
void ExecutorContext::switchToMpContext()
{
    VOLT_DEBUG("Switching context to partition %d on thread %p", mpEngineLocals.partitionId, pthread_self());
    savedEngineLocals = &enginesByPartitionId[getExecutorContext()->m_partitionId];
    pthread_setspecific(static_key, mpEngineLocals.context);
    ThreadLocalPool::assignThreadLocals(mpEngineLocals);
}

/*
 * Restore saved executor context.
 *
 * Must be paired with switchToMpContext().
 */
void ExecutorContext::restoreContext()
{
    VOLT_DEBUG("Restore context to partition %d on thread %p", savedEngineLocals.partitionId, pthread_self());
    pthread_setspecific(static_key, savedEngineLocals->context);
    ThreadLocalPool::assignThreadLocals(*savedEngineLocals);
    savedEngineLocals = NULL;
}

bool ExecutorContext::needContextRestore()
{
    return savedEngineLocals != NULL;
}

void ExecutorContext::bindToThread()
{
    pthread_setspecific(static_key, this);
    VOLT_DEBUG("Installing EC(%ld) for partition %d", (long)this, m_partitionId);
}

ExecutorContext* ExecutorContext::getExecutorContext()
{
    (void)pthread_once(&static_keyOnce, globalInitOrCreateOncePerProcess);
    return static_cast<ExecutorContext*>(pthread_getspecific(static_key));
}

UniqueTempTableResult ExecutorContext::executeExecutors(int subqueryId)
{
    const std::vector<AbstractExecutor*>& executorList = getExecutors(subqueryId);
    return executeExecutors(executorList, subqueryId);
}

UniqueTempTableResult ExecutorContext::executeExecutors(const std::vector<AbstractExecutor*>& executorList,
                                         int subqueryId)
{
    // Walk through the list and execute each plannode.
    // The query planner guarantees that for a given plannode,
    // all of its children are positioned before it in this list,
    // therefore dependency tracking is not needed here.
    size_t ttl = executorList.size();
    int ctr = 0;
    try {
        BOOST_FOREACH (AbstractExecutor *executor, executorList) {
            assert(executor);
            PlanNodeType nextPlanNodeType = executor->getPlanNode()->getPlanNodeType();
            if (nextPlanNodeType >= PLAN_NODE_TYPE_UPDATE && nextPlanNodeType <= PLAN_NODE_TYPE_SWAPTABLES) {
                AbstractOperationPlanNode* node = dynamic_cast<AbstractOperationPlanNode*>(executor->getPlanNode());
                assert(node);
                Table* targetTable = node->getTargetTable();
                PersistentTable *persistentTarget = dynamic_cast<PersistentTable*>(targetTable);
                if (persistentTarget != NULL && persistentTarget->isReplicatedTable()) {
                    if (mpEngineLocals.context == this) {
                        mpExecutor = executor;
                    }
                    VOLT_ERROR("PlanNodeType:%d", nextPlanNodeType);
                    if (SynchronizedThreadLock::countDownGlobalTxnStartCount()) {
                        switchToMpContext();
                        // Call the execute method to actually perform whatever action
                        // it is that the node is supposed to do...
                        if (!mpExecutor->execute(m_staticParams)) {
                            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                               "Unspecified execution error detected");
                        }
                        ++ctr;
                        mpExecutor = NULL;
                        // Assign the correct pool back to this thread
                        restoreContext();
                        VOLT_ERROR("release all waited thread");
                        SynchronizedThreadLock::signalLastSiteFinished();
                    } else {
                        VOLT_ERROR("wait for last site finished, then run next executor");
                        SynchronizedThreadLock::waitForLastSiteFinished();
                    }
                } else {
                    // Call the execute method to actually perform whatever action
                    // it is that the node is supposed to do...
                    if (!executor->execute(m_staticParams)) {
                        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                            "Unspecified execution error detected");
                    }
                    ++ctr;
                }
            } else {
                // Call the execute method to actually perform whatever action
                // it is that the node is supposed to do...
                if (!executor->execute(m_staticParams)) {
                    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                        "Unspecified execution error detected");
                }
                ++ctr;
            }
        }
    } catch (const SerializableEEException &e) {
        if (needContextRestore()) {
            // Assign the correct pool back to this thread
            restoreContext();
            SynchronizedThreadLock::signalLastSiteFinished();
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
        BOOST_FOREACH (AbstractExecutor *executor, executorList) {
            assert (executor);
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

    // Cleanup all but the temp table produced by the last executor.
    // The last temp table is the result which the caller may care about.
    for (int i = 0; i < executorList.size() - 1; ++i) {
        executorList[i]->cleanupTempOutputTable();
    }

    TempTable *result = executorList[ttl-1]->getPlanNode()->getTempOutputTable();
    return UniqueTempTableResult(result);
}

Table* ExecutorContext::getSubqueryOutputTable(int subqueryId) const
{
    const std::vector<AbstractExecutor*>& executorList = getExecutors(subqueryId);
    assert(!executorList.empty());
    return executorList.back()->getPlanNode()->getOutputTable();
}

void ExecutorContext::cleanupAllExecutors()
{
    typedef std::map<int, std::vector<AbstractExecutor*>* >::value_type MapEntry;
    BOOST_FOREACH(MapEntry& entry, *m_executorsMap) {
        int subqueryId = entry.first;
        cleanupExecutorsForSubquery(subqueryId);
    }

    // Clear any cached results from executed subqueries
    m_subqueryContextMap.clear();
}

void ExecutorContext::cleanupExecutorsForSubquery(const std::vector<AbstractExecutor*>& executorList) const {
    BOOST_FOREACH (AbstractExecutor *executor, executorList) {
        assert(executor);
        executor->cleanupTempOutputTable();
    }
}

void ExecutorContext::cleanupExecutorsForSubquery(int subqueryId) const
{
    const std::vector<AbstractExecutor*>& executorList = getExecutors(subqueryId);
    cleanupExecutorsForSubquery(executorList);
}

void ExecutorContext::resetExecutionMetadata(ExecutorVector* executorVector) {

    if (m_tuplesModifiedStack.size() != 0) {
        m_tuplesModifiedStack.pop();
    }
    assert (m_tuplesModifiedStack.size() == 0);

    executorVector->resetLimitStats();
}

void ExecutorContext::reportProgressToTopend(const TempTableLimits *limits) {

    int64_t allocated = limits != NULL ? limits->getAllocated() : -1;
    int64_t peak = limits != NULL ? limits->getPeakMemoryInBytes() : -1;

    //Update stats in java and let java determine if we should cancel this query.
    m_progressStats.TuplesProcessedInFragment += m_progressStats.TuplesProcessedSinceReport;
    int64_t tupleReportThreshold = m_topend->fragmentProgressUpdate(m_engine->getCurrentIndexInBatch(),
                                        m_progressStats.LastAccessedPlanNodeType,
                                        m_progressStats.TuplesProcessedInBatch + m_progressStats.TuplesProcessedInFragment,
                                        allocated,
                                        peak);
    m_progressStats.TuplesProcessedSinceReport = 0;

    if (tupleReportThreshold < 0) {
        VOLT_DEBUG("Interrupt query.");
        char buff[100];
        snprintf(buff, 100,
                "A SQL query was terminated after %.03f seconds because it exceeded the",
                static_cast<double>(tupleReportThreshold) / -1000.0);

        throw InterruptException(std::string(buff));
    }
    m_progressStats.TupleReportThreshold = tupleReportThreshold;
}

bool ExecutorContext::allOutputTempTablesAreEmpty() const {
    typedef std::map<int, std::vector<AbstractExecutor*>* >::value_type MapEntry;
    BOOST_FOREACH (MapEntry &entry, *m_executorsMap) {
        BOOST_FOREACH(AbstractExecutor* executor, *(entry.second)) {
            if (! executor->outputTempTableIsEmpty()) {
                return false;
            }
        }
    }
    return true;
}

void ExecutorContext::setDrStream(AbstractDRTupleStream *drStream) {
    assert (m_drStream != NULL);
    assert (drStream != NULL);
    assert (m_drStream->m_committedSequenceNumber >= drStream->m_committedSequenceNumber);
    int64_t lastCommittedSpHandle = std::max(m_lastCommittedSpHandle, drStream->m_openSpHandle);
    m_drStream->periodicFlush(-1L, lastCommittedSpHandle);
    int64_t oldSeqNum = m_drStream->m_committedSequenceNumber;
    m_drStream = drStream;
    m_drStream->setLastCommittedSequenceNumber(oldSeqNum);
}

void ExecutorContext::setDrReplicatedStream(AbstractDRTupleStream *drReplicatedStream) {
    assert (m_drReplicatedStream != NULL);
    assert (drReplicatedStream != NULL);
    assert (m_drReplicatedStream->m_committedSequenceNumber >= drReplicatedStream->m_committedSequenceNumber);
    int64_t lastCommittedSpHandle = std::max(m_lastCommittedSpHandle, drReplicatedStream->m_openSpHandle);
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
void ExecutorContext::checkTransactionForDR() {
    if (UniqueId::isMpUniqueId(m_uniqueId) && m_undoQuantum != NULL) {
        if (m_drStream) {
            if (m_drStream->transactionChecks(m_lastCommittedSpHandle,
                        m_spHandle, m_uniqueId))
            {
                m_undoQuantum->registerUndoAction(
                        new (*m_undoQuantum) DRTupleStreamUndoAction(m_drStream,
                                m_drStream->m_committedUso,
                                0));
            }
            if (m_drReplicatedStream) {
                if (m_drReplicatedStream->transactionChecks(m_lastCommittedSpHandle,
                            m_spHandle, m_uniqueId))
                {
                    m_undoQuantum->registerUndoAction(
                            new (*m_undoQuantum) DRTupleStreamUndoAction(
                                    m_drReplicatedStream,
                                    m_drReplicatedStream->m_committedUso,
                                    0));
                }
            }
        }
    }
}

} // end namespace voltdb
