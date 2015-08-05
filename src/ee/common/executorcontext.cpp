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
#include "common/executorcontext.hpp"

#include "common/debuglog.h"
#include "executors/abstractexecutor.h"

#include "boost/foreach.hpp"

#include <pthread.h>

using namespace std;

namespace voltdb {

static pthread_key_t static_key;
static pthread_once_t static_keyOnce = PTHREAD_ONCE_INIT;

static void createThreadLocalKey() {
    (void)pthread_key_create( &static_key, NULL);
}

ExecutorContext::ExecutorContext(int64_t siteId,
                CatalogId partitionId,
                UndoQuantum *undoQuantum,
                Topend* topend,
                Pool* tempStringPool,
                NValueArray* params,
                VoltDBEngine* engine,
                std::string hostname,
                CatalogId hostId,
                DRTupleStream *drStream,
                DRTupleStream *drReplicatedStream,
                CatalogId drClusterId) :
    m_topEnd(topend),
    m_tempStringPool(tempStringPool),
    m_undoQuantum(undoQuantum),
    m_staticParams(params),
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
    m_epoch(0) // set later
{
    (void)pthread_once(&static_keyOnce, createThreadLocalKey);
    bindToThread();
}

ExecutorContext::~ExecutorContext() {
    // currently does not own any of its pointers

    // There can be only one (per thread).
    assert(pthread_getspecific( static_key) == this);
    // ... or none, now that the one is going away.
    VOLT_DEBUG("De-installing EC(%ld)", (long)this);

    pthread_setspecific(static_key, NULL);
}

void ExecutorContext::bindToThread()
{
    // There can be only one (per thread).
    assert(pthread_getspecific(static_key) == NULL);
    pthread_setspecific(static_key, this);
    VOLT_DEBUG("Installing EC(%ld)", (long)this);
}


ExecutorContext* ExecutorContext::getExecutorContext() {
    (void)pthread_once(&static_keyOnce, createThreadLocalKey);
    return static_cast<ExecutorContext*>(pthread_getspecific(static_key));
}

Table* ExecutorContext::executeExecutors(int subqueryId)
{
    const std::vector<AbstractExecutor*>& executorList = getExecutors(subqueryId);
    return executeExecutors(executorList, subqueryId);
}

Table* ExecutorContext::executeExecutors(const std::vector<AbstractExecutor*>& executorList,
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
            // Call the execute method to actually perform whatever action
            // it is that the node is supposed to do...
            if (!executor->execute(*m_staticParams)) {
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                    "Unspecified execution error detected");
            }
            ++ctr;
        }
    } catch (const SerializableEEException &e) {
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
    return executorList[ttl-1]->getPlanNode()->getOutputTable();
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

void ExecutorContext::cleanupExecutorsForSubquery(int subqueryId) const {
    const std::vector<AbstractExecutor*>& executorList = getExecutors(subqueryId);
    cleanupExecutorsForSubquery(executorList);
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

} // end namespace voltdb
