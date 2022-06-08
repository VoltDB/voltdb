/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "VoltDBEngine.h"

#include "ExecutorVector.h"

#include "catalog/cluster.h"
#include "catalog/column.h"
#include "catalog/columnref.h"
#include "catalog/function.h"
#include "catalog/functionparameter.h"
#include "catalog/index.h"
#include "catalog/materializedviewhandlerinfo.h"
#include "catalog/materializedviewinfo.h"
#include "catalog/planfragment.h"
#include "catalog/statement.h"
#include "catalog/table.h"

#include "common/ElasticHashinator.h"
#include "common/ExecuteWithMpMemory.h"
#include "common/InterruptException.h"
#include "common/TupleOutputStream.h"
#include "common/TupleOutputStreamProcessor.h"

#include "common/SynchronizedThreadLock.h"
#include "executors/abstractexecutor.h"
#include "expressions/functionexpression.h"

#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"

#include "storage/AbstractDRTupleStream.h"
#include "storage/DRTupleStream.h"
#include "storage/ExecuteTaskUndoGenerateDREventAction.h"
#include "storage/MaterializedViewHandler.h"
#include "storage/MaterializedViewTriggerForWrite.h"
#include "storage/streamedtable.h"
#include "storage/SystemTableFactory.h"
#include "storage/TableCatalogDelegate.hpp"
#include "storage/tablefactory.h"
#include "storage/temptable.h"
#include "storage/ConstraintFailureException.h"
#include "storage/DRTupleStream.h"
#include "storage/TopicTupleStream.h"

#include "topics/GroupStore.h"

#if !defined(NDEBUG) && defined(MACOSX)
// Mute EXC_BAD_ACCESS in the debug mode for running LLDB.
#include <mach/task.h>
#include <mach/mach_init.h>
#include <mach/mach_port.h>
#endif

#include "org_voltdb_jni_ExecutionEngine.h" // to use static values

// The next #define limits the number of features pulled into the build
// We don't use those features.
#define BOOST_MULTI_INDEX_DISABLE_SERIALIZATION
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/sequenced_index.hpp>

#include <chrono> // For measuring the execution time of each fragment.
#if __cplusplus >= 201103L
#include <atomic>
#else
#include <cstdatomic>
#endif

ENABLE_BOOST_FOREACH_ON_CONST_MAP(Column);
ENABLE_BOOST_FOREACH_ON_CONST_MAP(Index);
ENABLE_BOOST_FOREACH_ON_CONST_MAP(MaterializedViewInfo);
ENABLE_BOOST_FOREACH_ON_CONST_MAP(Table);
ENABLE_BOOST_FOREACH_ON_CONST_MAP(Function);

static const size_t PLAN_CACHE_SIZE = 1000;
// table name prefix of DR conflict table
const std::string DR_REPLICATED_CONFLICT_TABLE_NAME = "VOLTDB_AUTOGEN_XDCR_CONFLICTS_REPLICATED";
const std::string DR_PARTITIONED_CONFLICT_TABLE_NAME = "VOLTDB_AUTOGEN_XDCR_CONFLICTS_PARTITIONED";

namespace voltdb {

// These typedefs prevent confusion in the parsing of BOOST_FOREACH.
typedef std::pair<std::string, TableCatalogDelegate*> LabeledTCD;
typedef std::pair<std::string, catalog::Column*> LabeledColumn;
typedef std::pair<std::string, catalog::Index*> LabeledIndex;
typedef std::pair<std::string, catalog::Table*> LabeledTable;
typedef std::pair<std::string, catalog::MaterializedViewInfo*> LabeledView;
typedef std::pair<std::string, catalog::Function*> LabeledFunction;
typedef std::pair<std::string, StreamedTable*> LabeledStream;
typedef std::pair<std::string, ExportTupleStream*> LabeledStreamWrapper;

/**
 * The set of plan bytes is explicitly maintained in MRU-first order,
 * while also indexed by the plans' bytes. Here lie boost-related dragons.
 */
using PlanSet = boost::multi_index::multi_index_container<
    boost::shared_ptr<ExecutorVector>,
    boost::multi_index::indexed_by<
        boost::multi_index::sequenced<>,
        boost::multi_index::hashed_unique<
            boost::multi_index::const_mem_fun<ExecutorVector,int64_t,&ExecutorVector::getFragId>>>>;

int32_t s_exportFlushTimeout=4000;  // export/tuple flush interval ms setting

/// This class wrapper around a typedef allows forward declaration as in scoped_ptr<EnginePlanSet>.
class EnginePlanSet : public PlanSet { };

VoltEEExceptionType VoltDBEngine::s_loadTableException =
    VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_NONE;
int VoltDBEngine::s_drHiddenColumnSize = 0;

AbstractDRTupleStream* VoltDBEngine::s_drReplicatedStream = nullptr;

VoltDBEngine::VoltDBEngine(Topend* topend, LogProxy* logProxy) : m_logManager(logProxy), m_topend(topend) {
    loadBuiltInJavaFunctions();
}

void
VoltDBEngine::initialize(
        int32_t clusterIndex,
        int64_t siteId,
        int32_t partitionId,
        int32_t sitesPerHost,
        int32_t hostId,
        std::string const& hostname,
        int32_t drClusterId,
        int32_t defaultDrBufferSize,
        bool drIgnoreConflicts,
        int32_t drCrcErrorIgnoreMax,
        bool drCrcErrorIgnoreFatal,
        int64_t tempTableMemoryLimit,
        bool isLowestSite,
        int32_t compactionThreshold) {
    m_clusterIndex = clusterIndex;
    m_siteId = siteId;
    m_isLowestSite = isLowestSite;
    m_partitionId = partitionId;
    m_tempTableMemoryLimit = tempTableMemoryLimit;
    m_compactionThreshold = compactionThreshold;

    // Instantiate our catalog - it will be populated later on by load()
    m_catalog.reset(new catalog::Catalog());

    // create the template single long (int) table
    vassert(m_templateSingleLongTable == NULL);
    m_templateSingleLongTable = new char[m_templateSingleLongTableSize];
    memset(m_templateSingleLongTable, 0, m_templateSingleLongTableSize);
    m_templateSingleLongTable[7] = 43;  // size through start of data?
    m_templateSingleLongTable[11] = 23; // size of header
    m_templateSingleLongTable[13] = 0;  // status code
    m_templateSingleLongTable[14] = 1;  // number of columns
    m_templateSingleLongTable[15] = static_cast<char>(ValueType::tBIGINT); // column type
    m_templateSingleLongTable[19] = 15; // column name length:  "modified_tuples" == 15

    strcpy(&m_templateSingleLongTable[20], "modified_tuples");

    m_templateSingleLongTable[38] = 1; // row count
    m_templateSingleLongTable[42] = 8; // row size

    if (drIgnoreConflicts) {
        m_wrapper.enableIgnoreConflicts();
    }
    m_wrapper.setCrcErrorIgnoreMax(drCrcErrorIgnoreMax);
    m_wrapper.setCrcErrorIgnoreFatal(drCrcErrorIgnoreFatal);

    // configure DR stream
    m_drStream = new DRTupleStream(partitionId, static_cast<size_t>(defaultDrBufferSize));

    // required for catalog loading.
    m_executorContext = new ExecutorContext(siteId, m_partitionId, m_currentUndoQuantum, getTopend(),
            &m_stringPool, this, hostname, hostId, m_drStream, m_drReplicatedStream, drClusterId);
    // Add the engine to the global list tracking replicated tables
    SynchronizedThreadLock::lockReplicatedResourceForInit();
    ThreadLocalPool::setPartitionIds(m_partitionId);
    VOLT_DEBUG("Initializing partition %d (tid %ld) with context %p", m_partitionId,
            SynchronizedThreadLock::getThreadId(), m_executorContext);
    EngineLocals newLocals = EngineLocals(ExecutorContext::getExecutorContext());
    SynchronizedThreadLock::init(sitesPerHost, newLocals);
    SynchronizedThreadLock::unlockReplicatedResourceForInit();
    m_groupStore.reset(new topics::GroupStore());
}

VoltDBEngine::~VoltDBEngine() {
    // WARNING WARNING WARNING
    // The sequence below in which objects are cleaned up/deleted is
    // fragile.  Reordering or adding additional destruction below
    // greatly increases the risk of accidentally freeing the same
    // object multiple times.  Change at your own risk.
    // --izzy 8/19/2009

#ifdef VOLT_POOL_CHECKING
    m_destroying = true;
    m_tlPool.shutdown();
    m_groupStore.reset(nullptr);
#endif
    // clean up execution plans
    m_plans.reset();

    // Clear the undo log before deleting the persistent tables so
    // that the persistent table schema are still around so we can
    // actually find the memory that has been allocated to non-inlined
    // strings and deallocated it.
    m_undoLog.clear();

    // clean up memory for the template memory for the single long (int) table
    delete[] m_templateSingleLongTable;

    // Delete table delegates and release any table reference counts.

    if (m_partitionId != 16383) {
        for (auto tcdIter = m_catalogDelegates.cbegin(); tcdIter != m_catalogDelegates.cend(); ) {
            auto eraseThis = tcdIter;
            tcdIter++;
            auto table = eraseThis->second->getPersistentTable();
            bool deleteWithMpPool = false;
            if (!table) {
                VOLT_DEBUG("Partition %d Deallocating %s table", m_partitionId, eraseThis->second->getTable()->name().c_str());
            } else if(!table->isReplicatedTable()) {
                VOLT_DEBUG("Partition %d Deallocating partitioned table %s", m_partitionId, eraseThis->second->getTable()->name().c_str());
            } else {
                deleteWithMpPool = true;
                VOLT_DEBUG("Partition %d Deallocating replicated table %s", m_partitionId, eraseThis->second->getTable()->name().c_str());
            }

            if (deleteWithMpPool) {
                if (isLowestSite()) {
                    ScopedReplicatedResourceLock scopedLock;
                    ExecuteWithMpMemory usingMpMemory;
                    delete eraseThis->second;
                }
            } else {
                delete eraseThis->second;
            }

            m_catalogDelegates.erase(eraseThis->first);
        }

        for (auto& entry : m_systemTables) {
            entry.second->decrementRefcount();
        }
        m_systemTables.clear();

        if (isLowestSite()) {
            SynchronizedThreadLock::resetMemory(SynchronizedThreadLock::s_mpMemoryPartitionId
#ifdef VOLT_POOL_CHECKING
               , m_destroying
#endif
            );
        }
        for (auto tid : m_snapshottingTables) {
            tid.second->decrementRefcount();
        }

        for (auto labeledInfo: m_functionInfo) {
            delete labeledInfo.second;
        }

        delete m_executorContext;

        if (isLowestSite()) {
            delete s_drReplicatedStream;
            s_drReplicatedStream = nullptr;
        }
        delete m_drStream;
    } else {
        delete m_executorContext;
    }
    VOLT_DEBUG("finished deallocate for partition %d", m_partitionId);
}

bool VoltDBEngine::decommission(bool remove, bool promote, int newSitePerHost) {
    VOLT_DEBUG("start decommission for partition %d, site % " PRId64, m_partitionId, m_siteId);
    {
        ConditionalSynchronizedExecuteWithMpMemory possiblySynchronizedUseMpMemory
                (true, isLowestSite(), []() {});
        if (possiblySynchronizedUseMpMemory.okToExecute()) {
            VOLT_DEBUG("on lowest site");
            // update site per host count for next countdown latch
            SynchronizedThreadLock::updateSitePerHost(newSitePerHost);
            // give up lowest site role
            if (remove) {
                m_isLowestSite = false;
                if (m_drReplicatedStream) {
                    m_drReplicatedStream = nullptr;
                    m_executorContext->setDrReplicatedStream(m_drReplicatedStream);
                }
            }
        } else {
            VOLT_DEBUG("on non lowest site");
            if (promote) {
                m_isLowestSite = true;
                if (s_drReplicatedStream) {
                    m_drReplicatedStream = s_drReplicatedStream;
                    m_executorContext->setDrReplicatedStream(m_drReplicatedStream);
                }
                SynchronizedThreadLock::swapContextforMPEngine();

            }
        }
    }
    if (remove) {
        cleanup();
        VOLT_DEBUG("Deactive EngineLocals %d", m_partitionId);
        SynchronizedThreadLock::deactiveEngineLocals(m_partitionId);
    }
    return true;
}

void VoltDBEngine::cleanup() {
    // clean up execution plans
    m_plans.reset();

    // Clear the undo log before deleting the persistent tables so
    // that the persistent table schema are still around so we can
    // actually find the memory that has been allocated to non-inlined
    // strings and deallocated it.
    m_undoLog.clear();
}

// ------------------------------------------------------------------
// OBJECT ACCESS FUNCTIONS
// ------------------------------------------------------------------
catalog::Catalog* VoltDBEngine::getCatalog() const {
    return m_catalog.get();
}

Table* VoltDBEngine::getTableById(int32_t tableId) const {
    // Caller responsible for checking null return value.
    return findInMapOrNull(tableId, m_tables);
}

Table* VoltDBEngine::getTableByName(const std::string& name) const {
    // Caller responsible for checking null return value.
    return findInMapOrNull(name, m_tablesByName);
}

StreamedTable* VoltDBEngine::getStreamTableByName(const std::string& name) const {
    return findInMapOrNull(name, m_exportingTables);
}

void VoltDBEngine::setStreamTableByName(std::string const& name, StreamedTable* newStreamTable) {
    vassert(findInMapOrNull(name, m_exportingTables) != NULL);
    m_exportingTables[name] = newStreamTable;
}

TableCatalogDelegate* VoltDBEngine::getTableDelegate(const std::string& name) const {
    // Caller responsible for checking null return value.
    return findInMapOrNull(name, m_delegatesByName);
}

catalog::Table* VoltDBEngine::getCatalogTable(const std::string& name) const {
    // iterate over all of the tables in the new catalog
    for (LabeledTable labeledTable : m_database->tables()) {
        auto catalogTable = labeledTable.second;
        if (catalogTable->name() == name) {
            return catalogTable;
        }
    }
    return NULL;
}

void VoltDBEngine::serializeTable(int32_t tableId, SerializeOutput& out) const {
    // Just look in our list of tables
    Table* table = getTableById(tableId);
    if (! table) {
        throwFatalException("Unable to find table for TableId '%d'", (int) tableId);
    }
    table->serializeTo(out);
}

PersistentTable* VoltDBEngine::getSystemTable(const SystemTableId id) const {
    return findInMapOrNull(id, m_systemTables);
}

// ------------------------------------------------------------------
// EXECUTION FUNCTIONS
// ------------------------------------------------------------------
/**
 * Execute the given fragments serially and in order.
 * Return 0 if there are no failures and n>0 if there are
 * any failures.  Note that this is the meat of the JNI
 * call org.voltdb.jni.ExecutionEngine.nativeExecutePlanFragments.
 *
 * @param numFragments          The number of fragments to execute.
 *                              This is directly from the JNI call.
 * @param planfragmentIds       The array of fragment ids.  This is
 *                              This is indirectly from the JNI call,
 *                              but has been translated from Java to
 *                              C++.
 * @param inputDependencyIds
 * @param serialInput           A SerializeInput object containing the parameters.
 *                              The JNI call has an array of Java Objects.  These
 *                              have been serialized and stuffed into a byte buffer
 *                              which is shared between the EE and the JVM.  This
 *                              shared buffer is in the engine on the EE side, and
 *                              in a pool of ByteBuffers on the Java side.  The
 *                              Java byte buffer pools own these, but the EE can
 *                              use them.
 * @param txnId                 The transaction id.  This comes from the JNI call directly.
 * @param spHandle
 * @param lastCommittedSpHandle The handle of the last committed single partition handle.
 *                              This is directly from the JNI call.
 * @param uniqueId              The unique id, taken directly from the JNI call.
 * @param undoToken             The undo token, taken directly from
 *                              the JNI call
 * @param traceOn               True to turn per-transaction tracing on.
 */
int VoltDBEngine::executePlanFragments(
      int32_t numFragments,
      int64_t planfragmentIds[],
      int64_t inputDependencyIds[],
      ReferenceSerializeInputBE &serialInput,
      int64_t txnId,
      int64_t spHandle,
      int64_t lastCommittedSpHandle,
      int64_t uniqueId,
      int64_t undoToken,
      bool traceOn) {
    // count failures
    int failures = 0;

    setUndoToken(undoToken);

    // configure the execution context.
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(), txnId, spHandle,
            lastCommittedSpHandle, uniqueId, traceOn);

    bool hasDRBinaryLog = m_executorContext->checkTransactionForDR();

    NValueArray &params = m_executorContext->getParameterContainer();

    // Reserve the space to track the number of succeeded fragments.
    size_t succeededFragmentsCountOffset = m_perFragmentStatsOutput.reserveBytes(sizeof(int32_t));
    // All the time measurements use nanoseconds.
    std::chrono::high_resolution_clock::time_point startTime, endTime;
    std::chrono::duration<int64_t, std::nano> elapsedNanoseconds;
    ReferenceSerializeInputBE perFragmentStatsBufferIn(
            getPerFragmentStatsBuffer(), getPerFragmentStatsBufferCapacity());
    // There is a byte at the very begining of the per-fragment stats buffer indicating
    // whether the time measurements should be enabled for the current batch.
    // If the current procedure invocation is not sampled, all its batches will not be timed.
    bool perFragmentTimingEnabled = perFragmentStatsBufferIn.readByte() > 0;

    /*
    * Reserve space in the result output buffer for the number of
    * result dependencies and for the dirty byte. Necessary for a
    * plan fragment because the number of produced depenencies may
    * not be known in advance.
    * Also reserve space for counting DR Buffer used space
    */
    m_startOfResultBuffer = m_resultOutput.reserveBytes(sizeof(int8_t) + sizeof(int32_t) + sizeof(int32_t));
    m_dirtyFragmentBatch = false;
    for (m_currentIndexInBatch = 0; m_currentIndexInBatch < numFragments; ++m_currentIndexInBatch) {
        int usedParamcnt = serialInput.readShort();
        m_executorContext->setUsedParameterCount(usedParamcnt);
        if (usedParamcnt < 0) {
            throwFatalException("parameter count is negative: %d", usedParamcnt);
        }
        vassert(usedParamcnt <= MAX_PARAM_COUNT);

        for (int j = 0; j < usedParamcnt; ++j) {
            params[j].deserializeFromAllocateForStorage(serialInput, &m_stringPool);
        }

        if (perFragmentTimingEnabled) {
            startTime = std::chrono::high_resolution_clock::now();
        }


        // success is 0 and error is 1.
        if (executePlanFragment(planfragmentIds[m_currentIndexInBatch],
                                inputDependencyIds ? inputDependencyIds[m_currentIndexInBatch] : -1,
                                traceOn)) {
            ++failures;
        }
        if (perFragmentTimingEnabled) {
            endTime = std::chrono::high_resolution_clock::now();
            elapsedNanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime);
            // Write the execution time to the per-fragment stats buffer.
            m_perFragmentStatsOutput.writeLong(elapsedNanoseconds.count());
        }
        if (failures > 0) {
            break;
        }

        // at the end of each frag, rollup and reset counters
        m_executorContext->m_progressStats.rollUpForPlanFragment();

        m_stringPool.purge();
    }

    if (failures == 0) {
        // write dirty-ness of the batch and number of dependencies output to the FRONT of
        // the result buffer
        m_resultOutput.writeBoolAt(m_startOfResultBuffer, m_dirtyFragmentBatch);
        size_t drBufferChange = size_t(0);
        if (hasDRBinaryLog) {
            if (m_drStream) {
                drBufferChange = m_drStream->getUso() - m_drStream->getCommittedUso();
                vassert(drBufferChange >= DRTupleStream::BEGIN_RECORD_SIZE);
                drBufferChange -= DRTupleStream::BEGIN_RECORD_SIZE;
            }
            if (m_drReplicatedStream) {
                size_t drReplicatedStreamBufferChange = m_drReplicatedStream->getUso() - m_drReplicatedStream->getCommittedUso();
                vassert(drReplicatedStreamBufferChange >= DRTupleStream::BEGIN_RECORD_SIZE);
                drBufferChange += drReplicatedStreamBufferChange- DRTupleStream::BEGIN_RECORD_SIZE;
            }
        }
        m_resultOutput.writeIntAt(m_startOfResultBuffer + 1, static_cast<int32_t> (drBufferChange));
        VOLT_DEBUG("executePlanFragments : hasDRBinaryLog %d, drBufferChange %d", hasDRBinaryLog,
                   static_cast<int32_t> (drBufferChange));
        m_resultOutput.writeIntAt(m_startOfResultBuffer + 5,
                static_cast<int32_t>(m_resultOutput.position() - m_startOfResultBuffer) -
                sizeof(int32_t) - sizeof(int32_t) - sizeof(int8_t));

    }
    else {
        if (hasDRBinaryLog) {
            VOLT_DEBUG("VoltDBEngine::executePlanFragments() P%d  mpId=%d  failed transactions n_failures=%d",
                       this->m_partitionId, uniqueId, failures );
        }
    }
    m_perFragmentStatsOutput.writeIntAt(succeededFragmentsCountOffset, m_currentIndexInBatch);
    m_currentIndexInBatch = -1;
    // If we were expanding the UDF buffer too much, shrink it back a little bit.
    // We check this at the end of every batch execution. So we won't resize the buffer
    // too frequently if most of the workload in the same batch requires a much larger buffer.
    // We initiate the resizing work in EE because this is a common place where
    // both single-partition and multi-partition transactions can get.
    if (m_udfBufferCapacity > MAX_UDF_BUFFER_SIZE) {
        m_topend->resizeUDFBuffer(MAX_UDF_BUFFER_SIZE);
    }
    return failures;
}

int VoltDBEngine::executePlanFragment(int64_t planfragmentId, int64_t inputDependencyId, bool traceOn) {
    vassert(planfragmentId != 0);

    m_currentInputDepId = static_cast<int32_t>(inputDependencyId);

    /*
     * Reserve space in the result output buffer for the number of
     * result dependencies generated by this particular plan fragment.
     * Necessary for a plan fragment because the
     * number of produced dependencies may not be known in advance.
     */
    m_numResultDependencies = 0;
    size_t numResultDependenciesCountOffset = m_resultOutput.reserveBytes(4);

    // In version 5.0, fragments may trigger execution of other fragments.
    // (I.e., DELETE triggered by an insert to enforce ROW LIMIT)
    // This method only executes top-level fragments.
    vassert(m_executorContext->getModifiedTupleStackSize() == 0);

    int64_t tuplesModified = 0;
    try {
        // execution lists for planfragments are cached by planfragment id
        setExecutorVectorForFragmentId(planfragmentId);
        vassert(m_currExecutorVec);

        executePlanFragment(m_currExecutorVec, &tuplesModified);
    } catch (const SerializableEEException &e) {
        serializeException(e);
        m_currExecutorVec = NULL;
        m_currentInputDepId = -1;
        m_executorContext->cleanupAllExecutors();
        return ENGINE_ERRORCODE_ERROR;
    }

    // Most temp table state is cleaned up automatically, but for
    // subqueries, some results are cached to get better performance.
    // Clean this up now.
    m_executorContext->cleanupAllExecutors();

    // If we get here, we've completed execution successfully, or
    // recovered after an error.  In any case, we should be able to
    // assert that temp tables are now cleared.
    DEBUG_ASSERT_OR_THROW_OR_CRASH(m_executorContext->allOutputTempTablesAreEmpty(),
                                   "Output temp tables not cleaned up after execution");

    m_currExecutorVec = NULL;
    m_currentInputDepId = -1;

    // assume this is sendless dml
    if (m_numResultDependencies == 0) {
        // put the number of tuples modified into our simple table
        uint64_t changedCount = htonll(tuplesModified);
        memcpy(m_templateSingleLongTable + m_templateSingleLongTableSize - 8, &changedCount, sizeof(changedCount));
        m_resultOutput.writeBytes(m_templateSingleLongTable, m_templateSingleLongTableSize);
        m_numResultDependencies++;
    }

    //Write the number of result dependencies if necessary.
    m_resultOutput.writeIntAt(numResultDependenciesCountOffset, m_numResultDependencies);

    // if a fragment modifies any tuples, the whole batch is dirty
    if (tuplesModified > 0) {
        m_dirtyFragmentBatch = true;
    }

    return ENGINE_ERRORCODE_SUCCESS;
}

UniqueTempTableResult VoltDBEngine::executePlanFragment(
        ExecutorVector* executorVector, int64_t* tuplesModified) {
    UniqueTempTableResult result;
    // set this to zero for dml operations
    m_executorContext->pushNewModifiedTupleCounter();

    // execution lists for planfragments are cached by planfragment id
    try {
        // Launch the target plan through its top-most executor list.
        executorVector->setupContext(m_executorContext);
        result = m_executorContext->executeExecutors(0);
    } catch (const SerializableEEException &e) {
        m_executorContext->resetExecutionMetadata(executorVector);
        throw;
    }

    if (tuplesModified != NULL) {
        *tuplesModified = m_executorContext->getModifiedTupleCount();
    }

    m_executorContext->resetExecutionMetadata(executorVector);

    VOLT_DEBUG("Finished executing successfully on partition %d.", m_partitionId);
    return result;
}

NValue VoltDBEngine::callJavaUserDefinedFunction(int32_t functionId, std::vector<NValue>& arguments) {
    UserDefinedFunctionInfo *info = findInMapOrNull(functionId, m_functionInfo);
    if (info == NULL) {
        // There must be serious inconsistency in the catalog if this could happen.
        throwFatalException("The execution engine lost track of the user-defined function (id = %d)", functionId);
    }

    // Estimate the size of the buffer we need. We will put:
    //   * size of the buffer (function ID + parameters)
    //   * function ID (int32_t)
    //   * parameters.
    size_t bufferSizeNeeded = sizeof(int32_t); // size of the function id.
    for (int index = 0; index < arguments.size(); ++index) {
        // It is very common that the argument we are going to pass is in
        // a compatible data type which does not exactly match the type that
        // is defined in the function.
        // We need to cast it to the target data type before the serialization.
        arguments[index] = arguments[index].castAs(info->paramTypes[index]);
        bufferSizeNeeded += arguments[index].serializedSize();
    }

    // Check buffer size here.
    // Adjust the buffer size when needed.
    // Note that bufferSizeNeeded does not include its own size.
    // So we are testing bufferSizeNeeded + sizeof(int32_t) here.
    if (bufferSizeNeeded + sizeof(int32_t) > m_udfBufferCapacity) {
        m_topend->resizeUDFBuffer(bufferSizeNeeded + sizeof(int32_t));
    }
    resetUDFOutputBuffer();

    // Serialize buffer size, function ID.
    m_udfOutput.writeInt(bufferSizeNeeded);
    m_udfOutput.writeInt(functionId);

    // Serialize UDF parameters to the buffer.
    for (auto const& value : arguments) {
        value.serializeTo(m_udfOutput);
    }
    // Make sure we did the correct size calculation.
    vassert(bufferSizeNeeded + sizeof(int32_t) == m_udfOutput.position());

    // callJavaUserDefinedFunction() will inform the Java end to execute the
    // Java user-defined function according to the function ID and the parameters
    // stored in the shared buffer. It will return 0 if the execution is successful.
    int32_t returnCode = m_topend->callJavaUserDefinedFunction();
    // Note that the buffer may already be resized after the execution.
    ReferenceSerializeInputBE udfResultIn(m_udfBuffer, m_udfBufferCapacity);
    if (returnCode == 0) {
        // After the the invocation, read the return value from the buffer.
        NValue retval = ValueFactory::getNValueOfType(info->returnType);
        retval.deserializeFromAllocateForStorage(udfResultIn, &m_stringPool);
        return retval;
    } else {
        // Error handling
        std::string errorMsg = udfResultIn.readTextString();
        throw SQLException(SQLException::volt_user_defined_function_error, errorMsg);
    }
}

/**
 * This function serialize the following information to the buffer and pass them to the Java side
 *
 * @param  functionId      The id of the user-defined aggregate function
 * @param  argument        This argument is a value in the table for the specified column (assemble method),
 *                         or it can be the byte array that represents the output from a worker (combine method),
 *                         or it can be NULL value (end method)
 * @param  type            The type of the arguments in argVector
 *                         It can be the type for the specified column (assemble method),
 *                         or it can be varbinary (combine method), or invalid (end method)
 * @param udafIndex        The index for the same user-defined aggregate function in a query.
 *                         For example, for "SELECT udf(b), avg(c), udf(a) FROM t",
 *                         udf(b) has an udafIndex of 0, and udf(a) has an udafIndex of 1.
 */
void VoltDBEngine::serializeToUDFOutputBuffer(int32_t functionId, const NValue& argument,
        ValueType type, int32_t udafIndex) {
    // Estimate the size of the buffer we need. We will put:
    //   * buffer size needed
    //   * function id (int32_t)
    //   * udaf index (int32_t)
    //   * a single parameter (NValue).

    // three int32_t: size of the buffer, function id, and udaf index
    int32_t bufferSizeNeeded = 3 * sizeof(int32_t);
    NValue cast_argument;
    if (type != ValueType::tINVALID) {
        cast_argument = argument.castAs(type);
        bufferSizeNeeded += cast_argument.serializedSize();
    }

    // Check buffer size here.
    // Adjust the buffer size when needed.
    if (bufferSizeNeeded > m_udfBufferCapacity) {
        m_topend->resizeUDFBuffer(bufferSizeNeeded);
    }
    resetUDFOutputBuffer();

    // size of data
    m_udfOutput.writeInt(bufferSizeNeeded - sizeof(int32_t));

    // Serialize function ID, and udaf index
    m_udfOutput.writeInt(functionId);
    m_udfOutput.writeInt(udafIndex);

    if (type != ValueType::tINVALID) {
        cast_argument.serializeTo(m_udfOutput);
    }

    // Make sure we did the correct size calculation.
    assert(bufferSizeNeeded == m_udfOutput.position());
}

/**
 * This function serialize the following information to the buffer and pass them to the Java side
 *
 * @param  functionId      The id of the user-defined aggregate function
 * @param  argVector       An array of arguments.
 *                         Each argument is a value in the table for the specified column (assemble method),
 *                         or it can be the byte array that represents the output from a worker (combine method),
 *                         or it can be NULL value (end method)
 * @param  argCount        The number of arguments in the argVector.
 *                         It is no larger than the size of argVector.
 * @param  type            The type of the arguments in argVector
 *                         It can be the type for the specified column (assemble method),
 *                         or it can be varbinary (combine method), or invalid (end method)
 * @param udafIndex        The index for the same user-defined aggregate function in a query.
 *                         For example, for "SELECT udf(b), avg(c), udf(a) FROM t",
 *                         udf(b) has an udafIndex of 0, and udf(a) has an udafIndex of 1.
 */
void VoltDBEngine::serializeToUDFOutputBuffer(int32_t functionId,
        std::vector<NValue> const& argVector, int32_t argCount, ValueType type, int32_t udafIndex) {
    // Determined the buffer size needed.
    // Information put in the buffer (sequentially)
    // * buffer size needed (int32_t)
    // * function id (int32_t)
    // * udaf index (int32_t)
    // * row count (int32_t)
    // * a list of rows coresponding to a given column (NValue)

    // Make sure the argCount is no larger than the size of argVector
    assert(argCount <= argVector.size());

    int32_t bufferSizeNeeded = 4 * sizeof(int32_t);
    std::vector<NValue> cast_argument(argCount);
    if (type != ValueType::tINVALID) {
        for (int i = 0; i < argCount; i++) {
            cast_argument[i] = argVector[i].castAs(type);
            bufferSizeNeeded += cast_argument[i].serializedSize();
        }
    }

    // Check buffer size here.
    // Adjust the buffer size when needed.
    if (bufferSizeNeeded > m_udfBufferCapacity) {
        m_topend->resizeUDFBuffer(bufferSizeNeeded);
    }
    resetUDFOutputBuffer();

    // size of data
    m_udfOutput.writeInt(bufferSizeNeeded - sizeof(int32_t));

    // serialize data
    m_udfOutput.writeInt(functionId);
    m_udfOutput.writeInt(udafIndex);
    m_udfOutput.writeInt(argCount);

    if (type != ValueType::tINVALID) {
        for (int i = 0; i < argCount; i++) {
            cast_argument[i].serializeTo(m_udfOutput);
        }
    }
    // Make sure we did the correct size calculation.
    assert(bufferSizeNeeded == m_udfOutput.position());
}

void VoltDBEngine::checkUserDefinedFunctionInfo(UserDefinedFunctionInfo *info, int32_t functionId) {
    if (info == nullptr) {
        // There must be serious inconsistency in the catalog if this could happen.
        throwFatalException("The execution engine lost track of the user-defined function (id = %d)",
                functionId);
    }
}

void VoltDBEngine::checkJavaFunctionReturnCode(int32_t returnCode, const char* name) {
    if (returnCode != 0) {
        throwSQLException(
                SQLException::volt_user_defined_function_error,
                "%s failed: %s",
                name,
                ReferenceSerializeInputBE(m_udfBuffer, m_udfBufferCapacity).readTextString().c_str());
    }
}

NValue VoltDBEngine::udfResultHelper(int32_t returnCode, bool partition_table, ValueType type) {
    ReferenceSerializeInputBE udfResultIn(m_udfBuffer, m_udfBufferCapacity);
    if (returnCode == 0) {
        // After the the invocation, read the return value from the buffer.
        NValue retval;
        if (partition_table) {
            // if this is a partitioned table, the returnType here
            // is a varbinary since we serialized the worker's
            // output on the java side
            retval = ValueFactory::getNValueOfType(ValueType::tVARBINARY);
        } else {
            // if this is a replicated table, the returnType will just be
            // the final returnType in the end method
            retval = ValueFactory::getNValueOfType(type);
        }
        retval.deserializeFromAllocateForStorage(udfResultIn, &m_stringPool);
        return retval;
    } else { // Error handling
        throw SQLException(SQLException::volt_user_defined_function_error,
                udfResultIn.readTextString());
    }
}

void VoltDBEngine::callJavaUserDefinedAggregateStart(int32_t functionId) {
    checkUserDefinedFunctionInfo(findInMapOrNull(functionId, m_functionInfo), functionId);
    checkJavaFunctionReturnCode(m_topend->callJavaUserDefinedAggregateStart(functionId),
            "UserDefinedAggregate::start()");
}

void VoltDBEngine::callJavaUserDefinedAggregateAssemble(
        int32_t functionId, std::vector<NValue>& argVector, int32_t argCount, int32_t udafIndex) {
    UserDefinedFunctionInfo *info = findInMapOrNull(functionId, m_functionInfo);
    checkUserDefinedFunctionInfo(info, functionId);
    serializeToUDFOutputBuffer(functionId, argVector, argCount,
            info->paramTypes.front(), udafIndex);
    checkJavaFunctionReturnCode(m_topend->callJavaUserDefinedAggregateAssemble(),
            "UserDefinedAggregate::assemble()");
}

void VoltDBEngine::callJavaUserDefinedAggregateCombine(
        int32_t functionId, const NValue& argument, int32_t udafIndex) {
    checkUserDefinedFunctionInfo(findInMapOrNull(functionId, m_functionInfo), functionId);
    // the argument here is of the type varbinary because this is the serialized byte
    // array after the worker end method
    serializeToUDFOutputBuffer(functionId, argument, ValueType::tVARBINARY, udafIndex);
    checkJavaFunctionReturnCode(m_topend->callJavaUserDefinedAggregateCombine(),
            "UserDefinedAggregate::combine()");
}

NValue VoltDBEngine::callJavaUserDefinedAggregateWorkerEnd(int32_t functionId, int32_t udafIndex) {
    UserDefinedFunctionInfo *info = findInMapOrNull(functionId, m_functionInfo);
    checkUserDefinedFunctionInfo(info, functionId);
    serializeToUDFOutputBuffer(functionId, NValue::getNullValue(ValueType::tINVALID),
            ValueType::tVARBINARY, udafIndex);
    return udfResultHelper(m_topend->callJavaUserDefinedAggregateWorkerEnd(), true, info->returnType);
}

NValue VoltDBEngine::callJavaUserDefinedAggregateCoordinatorEnd(int32_t functionId, int32_t udafIndex) {
    UserDefinedFunctionInfo *info = findInMapOrNull(functionId, m_functionInfo);
    checkUserDefinedFunctionInfo(info, functionId);
    serializeToUDFOutputBuffer(functionId, NValue::getNullValue(ValueType::tINVALID),
            ValueType::tINVALID, udafIndex);
    return udfResultHelper(m_topend->callJavaUserDefinedAggregateCoordinatorEnd(), false, info->returnType);
}


void VoltDBEngine::releaseUndoToken(int64_t undoToken, bool isEmptyDRTxn) {
    if (m_currentUndoQuantum != NULL && m_currentUndoQuantum->getUndoToken() == undoToken) {
        m_currentUndoQuantum = NULL;
        m_executorContext->setupForPlanFragments(NULL);
    }
    m_undoLog.release(undoToken);

    if (isEmptyDRTxn) {
        if (m_executorContext->drStream()) {
            m_executorContext->drStream()
                ->rollbackDrTo(m_executorContext->drStream()->getCommittedUso(), SIZE_MAX);
        }
        if (m_executorContext->drReplicatedStream()) {
            m_executorContext->drReplicatedStream()
                ->rollbackDrTo(m_executorContext->drReplicatedStream()->getCommittedUso(), SIZE_MAX);
        }
    } else {
        if (m_executorContext->drStream()) {
            m_executorContext->drStream()->endTransaction(m_executorContext->currentUniqueId());
        }
        if (m_executorContext->drReplicatedStream()) {
            m_executorContext->drReplicatedStream()
                ->endTransaction(m_executorContext->currentUniqueId());
        }
    }
}

void VoltDBEngine::undoUndoToken(int64_t undoToken) {
    m_currentUndoQuantum = NULL;
    m_executorContext->setupForPlanFragments(NULL);
    m_undoLog.undo(undoToken);
}

void VoltDBEngine::serializeException(const SerializableEEException& e) {
    resetReusedResultOutputBuffer();
    e.serialize(getExceptionOutputSerializer());
}

// -------------------------------------------------
// RESULT FUNCTIONS
// -------------------------------------------------
void VoltDBEngine::send(Table* dependency) {
    VOLT_DEBUG("Sending Dependency from C++");
    m_resultOutput.writeInt(-1); // legacy placeholder for old output id
    dependency->serializeTo(m_resultOutput);
    m_numResultDependencies++;
}

int VoltDBEngine::loadNextDependency(Table* destination) {
    return m_topend->loadNextDependency(m_currentInputDepId, &m_stringPool, destination);
}

// -------------------------------------------------
// Catalog Functions
// -------------------------------------------------
bool VoltDBEngine::updateCatalogDatabaseReference() {
    auto cluster = m_catalog->clusters().get("cluster");
    if (!cluster) {
        VOLT_ERROR("Unable to find cluster catalog information");
        return false;
    }

    m_database = cluster->databases().get("database");
    if (!m_database) {
        VOLT_ERROR("Unable to find database catalog information");
        return false;
    }
    m_isActiveActiveDREnabled = cluster->drRole() == "xdcr";
    s_drHiddenColumnSize = m_isActiveActiveDREnabled ? 8 : 0;

    return true;
}

bool VoltDBEngine::loadCatalog(const int64_t timestamp, const std::string &catalogPayload) {
    vassert(m_executorContext != NULL);
    ExecutorContext* executorContext = ExecutorContext::getExecutorContext();
    if (executorContext == NULL) {
        VOLT_DEBUG("Rebinding EC (%ld) to new thread", (long)m_executorContext);
        // It is the thread-hopping VoltDBEngine's responsibility to re-establish the EC for each new thread it runs on.
        m_executorContext->bindToThread();
    }

    VOLT_DEBUG("Loading catalog...%d", m_partitionId);
    if (m_partitionId == 16383) {
        // Don't allocate tables on the MP thread because the last SP thread will do that
        return true;
    }
    vassert(m_catalog != NULL);
    VOLT_DEBUG("Loading catalog on partition %d ...", m_partitionId);


    m_catalog->execute(catalogPayload);


    if (updateCatalogDatabaseReference() == false) {
        return false;
    }

    // Set DR flag based on current catalog state
    catalog::Cluster* catalogCluster = m_catalog->clusters().get("cluster");
    m_executorContext->drStream()->m_enabled = catalogCluster->drProducerEnabled();
    m_executorContext->drStream()->setFlushInterval(catalogCluster->drFlushInterval());
    if (m_executorContext->drReplicatedStream()) {
        m_executorContext->drReplicatedStream()->m_enabled = catalogCluster->drProducerEnabled();
        m_executorContext->drReplicatedStream()->setFlushInterval(catalogCluster->drFlushInterval());
    }
    s_exportFlushTimeout = catalogCluster->exportFlushInterval();

    VOLT_DEBUG("loading partitioned parts of catalog from partition %d", m_partitionId);

    createSystemTables();

    //When loading catalog we do isStreamUpdate to true as we are starting fresh or rejoining/recovering.
    std::map<std::string, ExportTupleStream*> purgedStreams;
    if (processCatalogAdditions(timestamp, false, true, purgedStreams) == false) {
        return false;
    }

    rebuildTableCollections();

    if (SynchronizedThreadLock::countDownGlobalTxnStartCount(isLowestSite())) {
        VOLT_TRACE("loading replicated parts of catalog from partition %d", m_partitionId);

        // load up all the tables, adding all tables
        if (processReplicatedCatalogAdditions(timestamp, true, purgedStreams) == false) {
            return false;
        }

        rebuildReplicatedTableCollections();

        // load up all the materialized views
        // and limit delete statements.
        //
        // This must be done after loading all the tables.
        VOLT_TRACE("loading replicated views from partition %d", m_partitionId);
        initReplicatedMaterializedViewsAndLimitDeletePlans();

        // Assign the correct pool back to this thread
        SynchronizedThreadLock::signalLowestSiteFinished();
    }

    VOLT_TRACE("loading partitioned views from partition %d", m_partitionId);
    // load up all the materialized views
    // and limit delete statements.
    //
    // This must be done after loading all the tables.
    initMaterializedViewsAndLimitDeletePlans();

    // Because Join views of partitioned tables could update the handler list of replicated tables we need to make
    // sure all partitions finish these updates before allowing other transactions to touch the replicated tables
    if (SynchronizedThreadLock::countDownGlobalTxnStartCount(isLowestSite())) {
        SynchronizedThreadLock::signalLowestSiteFinished();
    }
    VOLT_TRACE("Loaded catalog from partition %d ...", m_partitionId);
    return true;
}

/*
 * Obtain the recent deletion list from the catalog.  For any item in
 * that list with a corresponding table delegate, process a deletion.
 *
 * TODO: This should be extended to find the parent delegate if the
 * deletion isn't a top-level object .. and delegates should have a
 * deleteChildCommand() interface.
 *
 * Note, this only deletes tables, indexes are deleted in
 * processCatalogAdditions(..) for dumb reasons.
 */
void VoltDBEngine::processCatalogDeletes(int64_t timestamp, bool updateReplicated,
        std::map<std::string, ExportTupleStream*> & purgedStreams) {
    std::vector<std::string> deletion_vector;
    m_catalog->getDeletedPaths(deletion_vector);
    std::set<std::string> deletions(deletion_vector.begin(), deletion_vector.end());
    // Filter out replicated or partitioned deletions
    std::set<std::string>::iterator it = deletions.begin();
    while (it != deletions.end()) {
        const std::string& path = *it++;
        auto pos = m_catalogDelegates.find(path);
        if (pos == m_catalogDelegates.end()) {
           continue;
        }
        auto tcd = pos->second;
        vassert(tcd);
        if (tcd) {
            Table* table = tcd->getTable();
            PersistentTable* persistenttable = dynamic_cast<PersistentTable*>(table);
            if (persistenttable &&
                    updateReplicated != persistenttable->isReplicatedTable()) {
                deletions.erase(path);
            }
        }
    }

    // delete any empty persistent tables, forcing them to be rebuilt
    // (Unless the are actually being deleted -- then this does nothing)

    for (LabeledTCD delegatePair : m_catalogDelegates) {
        auto tcd = delegatePair.second;
        Table* table = tcd->getTable();

        // skip export tables for now
        if (dynamic_cast<StreamedTable*>(table)) {
            continue;
        } else if (table->activeTupleCount() == 0) {
            PersistentTable *persistenttable = dynamic_cast<PersistentTable*>(table);
            // identify empty tables and mark for deletion
            if (persistenttable && persistenttable->isReplicatedTable() == updateReplicated) {
                deletions.insert(delegatePair.first);
            }
        }
    }

    auto catalogFunctions = m_database->functions();

    // delete tables in the set
    bool isReplicatedTable;
    for (auto path : deletions) {
        // If the delete path is under the catalog functions item, drop the user-defined function.
        if (startsWith(path, catalogFunctions.path())) {
            catalog::Function* catalogFunction =
                    static_cast<catalog::Function*>(m_catalog->itemForRef(path));
            if (catalogFunction != NULL) {
#ifdef VOLT_DEBUG_ENABLED
                VOLT_DEBUG("UDFCAT: Deleting function info (ID = %d)", catalogFunction->functionId());
                auto funcInfo = m_functionInfo.find(catalogFunction->functionId());
                if (funcInfo == m_functionInfo.end()) {
                    VOLT_DEBUG("UDFCAT:    Cannot find the corresponding function info structure.");
                }
#endif
                delete m_functionInfo[catalogFunction->functionId()];
                m_functionInfo.erase(catalogFunction->functionId());
            }
            continue;
        }

        isReplicatedTable = false;
        auto pos = m_catalogDelegates.find(path);
        if (pos == m_catalogDelegates.end()) {
           continue;
        }
        auto tcd = pos->second;
        /*
         * Instruct the table to flush all export data
         * Then tell it about the new export generation/catalog txnid
         * which will cause it to notify the topend export data source
         * that no more data is coming for the previous generation
         */
        vassert(tcd);
        if (tcd) {
            Table* table = tcd->getTable();
            PersistentTable * persistenttable = dynamic_cast<PersistentTable*>(table);
            if (persistenttable) {
                // IW-ENG14804, handle deleting companion stream
                // FIXME: factorize deletion logic in common method
                auto streamedtable = persistenttable->getStreamedTable();
                if (streamedtable) {
                    VOLT_DEBUG("delete a streamed companion wrapper for %s", tcd->getTable()->name().c_str());
                    const std::string& name = streamedtable->name();
                    //Maintain the streams that will go away for which wrapper needs to be cleaned;
                    auto wrapper = streamedtable->getWrapper();
                    if (wrapper) {
                        purgedStreams[name] = streamedtable->getWrapper();
                        //Unset wrapper so it can be deleted after last push.
                        streamedtable->setWrapper(NULL);
                    }
                    m_exportingTables.erase(name);
                }
            }
            if (persistenttable && persistenttable->isReplicatedTable()) {
                isReplicatedTable = true;
                ExecuteWithAllSitesMemory execAllSites;
                for (auto engineIt : execAllSites) {
                    EngineLocals& curr = engineIt.second;
                    VoltDBEngine* currEngine = curr.context->getContextEngine();
                    SynchronizedThreadLock::assumeSpecificSiteContext(curr);
                    currEngine->m_delegatesByName.erase(table->name());
                }
            } else {
                m_delegatesByName.erase(table->name());
            }
            auto streamedtable = dynamic_cast<StreamedTable*>(table);
            if (streamedtable) {
                const std::string& name = streamedtable->name();
                //Maintain the streams that will go away for which wrapper needs to be cleaned;
                purgedStreams[name] = streamedtable->getWrapper();
                //Unset wrapper so it can be deleted after last push.
                streamedtable->setWrapper(nullptr);
                m_exportingTables.erase(name);
            }
            if (isReplicatedTable) {
                VOLT_TRACE("delete a REPLICATED table %s", tcd->getTable()->name().c_str());
                ExecuteWithMpMemory usingMpMemory;
                delete tcd;
            } else {
                VOLT_TRACE("delete a PARTITIONED table %s", tcd->getTable()->name().c_str());
                delete tcd;
            }
        }
        if (isReplicatedTable) {
            ExecuteWithAllSitesMemory execAllSites;
            for (auto engineIt : execAllSites) {
                EngineLocals& curr = engineIt.second;
                VoltDBEngine* currEngine = curr.context->getContextEngine();
                SynchronizedThreadLock::assumeSpecificSiteContext(curr);
                currEngine->m_catalogDelegates.erase(path);
            }
        } else {
            m_catalogDelegates.erase(path);
        }
    }
}

static bool haveDifferentSchema(
        catalog::Table* srcTable, voltdb::Table* targetTable, bool targetIsPersistentTable) {
    // covers column count
    if (srcTable->columns().size() != targetTable->columnCount()) {
        return true;
    } else if (targetIsPersistentTable) {
        auto* persistentTable = dynamic_cast<PersistentTable *>(targetTable);
        if (srcTable->isDRed() != persistentTable->isDREnabled() ||
                static_cast<TableType>(srcTable->tableType()) != persistentTable->getTableType()) {
           return true;
        }
    }

    // make sure each column has same metadata
    for (auto outerIter : srcTable->columns()) {
        int index = outerIter.second->index();
        int size = outerIter.second->size();
        auto const type = outerIter.second->type();
        const std::string& name = outerIter.second->name();
        bool nullable = outerIter.second->nullable();
        bool inBytes = outerIter.second->inbytes();

        if (targetTable->columnName(index).compare(name)) {
            return true;
        }

        auto columnInfo = targetTable->schema()->getColumnInfo(index);

        if (columnInfo->allowNull != nullable || static_cast<decltype(type)>(columnInfo->getVoltType()) != type) {
            return true;
        }

        // check the size of types where size matters
        if ((type == static_cast<decltype(type)>(ValueType::tVARCHAR) ||
                    type == static_cast<decltype(type)>(ValueType::tVARBINARY)) &&
                (columnInfo->length != size || columnInfo->inBytes != inBytes)) {
            vassert(columnInfo->inBytes == inBytes || type == static_cast<decltype(type)>(ValueType::tVARCHAR));
            return true;
        }
    }

    return false;
}

void VoltDBEngine::createSystemTables() {
    SystemTableFactory factory(m_compactionThreshold);
    for (SystemTableId id : SystemTableFactory::getAllSystemTableIds()) {
        vassert(m_systemTables.find(id) == m_systemTables.end());
        PersistentTable *table = factory.create(id);
        table->incrementRefcount();
        m_systemTables[id] = table;
    }

    m_groupStore->initialize(this);
}

/*
 * Rebuild all views on a table that are still in the catalog. This also removes views which were on the table but
 * no longer in the catalog
 */
template<class TableType, class MaterializedView>
inline void VoltDBEngine::rebuildViewsOnTable(catalog::Table* catalogTable, TableType* table) {
    std::vector<catalog::MaterializedViewInfo*> survivingInfos;
    std::vector<MaterializedView*> survivingViews;
    std::vector<MaterializedView*> obsoleteViews;

    const catalog::CatalogMap<catalog::MaterializedViewInfo>& views = catalogTable->views();

    MaterializedView::segregateMaterializedViews(table->views(),
            views.begin(), views.end(),
            survivingInfos, survivingViews, obsoleteViews);

    // This process temporarily duplicates the materialized view definitions and their
    // target table reference counts for all the right materialized view tables,
    // leaving the others to go away with the existingTable.
    // Since this is happening "mid-stream" in the redefinition of all of the source and target tables,
    // there needs to be a way to handle cases where the target table HAS been redefined already and
    // cases where it HAS NOT YET been redefined (and cases where it just survives intact).
    // At this point, the materialized view makes a best effort to use the
    // current/latest version of the table -- particularly, because it will have made off with the
    // "old" version's primary key index, which is used in the MaterializedView*Trigger constructor.
    // Once ALL tables have been added/(re)defined, any materialized view definitions that still use
    // an obsolete target table needs to be brought forward to reference the replacement table.
    // See initMaterializedViewsAndLimitDeletePlans

    for (int ii = 0; ii < survivingInfos.size(); ++ii) {
        auto currInfo = survivingInfos[ii];
        auto currView = survivingViews[ii];
        PersistentTable* oldDestTable = currView->destTable();
        // Use the now-current definition of the target table, to be updated later, if needed.
        auto targetDelegate = findInMapOrNull(oldDestTable->name(), m_delegatesByName);
        PersistentTable* destTable = oldDestTable; // fallback value if not (yet) redefined.
        if (targetDelegate) {
            auto newDestTable = targetDelegate->getPersistentTable();
            if (newDestTable) {
                destTable = newDestTable;
            }
        }

        ConditionalExecuteWithMpMemory useMpMemoryIfReplicated(catalogTable->isreplicated());
        // This guards its destTable from accidental deletion with a refcount bump.
        MaterializedView::build(table, destTable, currInfo);
        obsoleteViews.push_back(currView);
    }

    {
        ConditionalExecuteWithMpMemory useMpMemoryIfReplicated(catalogTable->isreplicated());
        for (auto toDrop : obsoleteViews) {
            table->dropMaterializedView(toDrop);
        }
    }
}

/*
 * Create catalog delegates for new catalog tables.
 * Create the tables themselves when new tables are needed.
 * Add and remove indexes if indexes are added or removed from an
 * existing table.
 * Use the txnId of the catalog update as the generation for export
 * data.
 */
bool VoltDBEngine::processCatalogAdditions(int64_t timestamp, bool updateReplicated,
        bool isStreamUpdate, std::map<std::string, ExportTupleStream*> & purgedStreams) {
    // iterate over all of the tables in the new catalog
    for (LabeledTable labeledTable : m_database->tables()) {
        // get the catalog's table object
        auto catalogTable = labeledTable.second;
        if (catalogTable->isreplicated() != updateReplicated) {
            // Only process the table if it matches the type to be updated
            continue;
        }

        // get the delegate for the table... add the table if it's null
        auto tcd = findInMapOrNull(catalogTable->path(), m_catalogDelegates);
        if (!tcd) {
            //////////////////////////////////////////
            // add a completely new table
            //////////////////////////////////////////
            if (updateReplicated) {
                vassert(SynchronizedThreadLock::isLowestSiteContext());
                ExecuteWithMpMemory useMpMemory;
                tcd = new TableCatalogDelegate(catalogTable->signature(), m_compactionThreshold, this);
                // use the delegate to init the table and create indexes n' stuff
                tcd->init(*m_database, *catalogTable, m_isActiveActiveDREnabled);
                const std::string& tableName = tcd->getTable()->name();
                VOLT_TRACE("add a REPLICATED completely new table or rebuild an empty table %s", tableName.c_str());
                {
                    ExecuteWithAllSitesMemory execAllSites;
                    for (auto engineIt : execAllSites) {
                        EngineLocals &curr = engineIt.second;
                        VoltDBEngine *currEngine = curr.context->getContextEngine();
                        currEngine->m_catalogDelegates[catalogTable->path()] = tcd;
                        currEngine->m_delegatesByName[tableName] = tcd;
                    }
                }
                vassert(tcd->getStreamedTable() == NULL);
            } else {
                tcd = new TableCatalogDelegate(catalogTable->signature(), m_compactionThreshold, this);
                // use the delegate to init the table and create indexes n' stuff
                tcd->init(*m_database, *catalogTable, m_isActiveActiveDREnabled);
                m_catalogDelegates[catalogTable->path()] = tcd;
                Table* table = tcd->getTable();
                VOLT_TRACE("add a PARTITIONED completely new table or rebuild an empty table %s", table->name().c_str());
                m_delegatesByName[table->name()] = tcd;

            }
            // set export info on the new table
            auto streamedTable = tcd->getStreamedTable();
            if (!streamedTable) {
                // Check if this table has a shadow stream
                PersistentTable *persistentTable = tcd->getPersistentTable();
                streamedTable = persistentTable->getStreamedTable();
                if (streamedTable) {
                    VOLT_DEBUG("setting up shadow stream for %s", persistentTable->name().c_str());
                }
            }
            if (streamedTable) {
                const std::string& name = streamedTable->name();
                if (tableTypeNeedsTupleStream(tcd->getTableType())) {
                    attachTupleStream(streamedTable, name, purgedStreams, timestamp);
                } else {
                    // The table/stream type has been changed
                    streamedTable->setWrapper(NULL);
                    streamedTable->setExportStreamPositions(0, 0, 0);
                }


            }
        } else {
            //////////////////////////////////////////////
            // add/modify/remove indexes that have changed
            //  in the catalog
            //////////////////////////////////////////////
            /*
             * Instruct the table that was not added but is being retained to flush
             * Then tell it about the new export generation/catalog txnid
             * which will cause it to notify the topend export data source
             * that no more data is coming for the previous generation
             */
            PersistentTable *persistentTable = tcd->getPersistentTable();
            bool tableSchemaChanged = false;

            if (persistentTable) {
                // Check if this table has a companion stream
                auto streamedTable = persistentTable->getStreamedTable();
                if (streamedTable) {
                    VOLT_DEBUG("Updating companion stream for %s", persistentTable->name().c_str());
                    const std::string& name = streamedTable->name();
                    if (tableTypeNeedsTupleStream(tcd->getTableType())) {
                        attachTupleStream(streamedTable, name, purgedStreams, timestamp);
                    }
                }
                tableSchemaChanged = haveDifferentSchema(catalogTable, persistentTable, true);

                // Update table type
                persistentTable->setTableType(static_cast<TableType>(catalogTable->tableType()));
            }

            auto streamedTable = tcd->getStreamedTable();
            if (streamedTable) {
                //Dont update and roll generation if this is just a non stream table update.
                if (isStreamUpdate) {
                    TableType tableType = tcd->getTableType();
                    TableType tableTypeUpdate = static_cast<TableType>(catalogTable->tableType());
                    if (tableType != tableTypeUpdate) {
                        tcd->setTableType(tableTypeUpdate);
                    }
                    bool streamHasWrapper = (streamedTable->getWrapper() != nullptr);
                    const std::string& name = streamedTable->name();
                    if (tableTypeNeedsTupleStream(tcd->getTableType())) {
                        attachTupleStream(streamedTable, name, purgedStreams, timestamp);
                        tableSchemaChanged = haveDifferentSchema(catalogTable, streamedTable, false);
                        // Wrapper can be reused. Update it here if no schema change.
                        // Otherwise update it after its schema is updated.
                        if (!tableSchemaChanged && streamHasWrapper) {
                            streamedTable->getWrapper()->update(*streamedTable, *m_database);
                        }
                    }
                    else {
                        detachTupleStream(streamedTable, name, purgedStreams);
                    }
                }

                // Deal with views if this is not a shadow stream
                rebuildViewsOnTable<StreamedTable, MaterializedViewTriggerForStreamInsert>(catalogTable, streamedTable);

                // note, this is the end of the line for export tables for now,
                // don't allow them to change schema yet
                if (!tableSchemaChanged) {
                    continue;
                }
            }

            //////////////////////////////////////////
            // if the persistent table schema has changed, build a new
            // table and migrate tuples over to it, repopulating
            // indexes as we go
            //////////////////////////////////////////

            if (tableSchemaChanged) {
                char msg[512];
                snprintf(msg, sizeof(msg), "Table %s has changed schema and will be rebuilt.",
                         catalogTable->name().c_str());
                msg[sizeof msg - 1] = '\0';
                LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_DEBUG, msg);
                ConditionalExecuteWithMpMemory useMpMemoryIfReplicated(updateReplicated);
                tcd->processSchemaChanges(*m_database, *catalogTable, m_delegatesByName, m_isActiveActiveDREnabled);
                // update exporting tables with new stream
                StreamedTable* stream = tcd->getStreamedTable();
                if (!stream) {
                   PersistentTable *persistenttable = dynamic_cast<PersistentTable*>(tcd->getTable());
                   if (persistenttable) {
                       stream = persistenttable->getStreamedTable();
                   }
                }

                if (stream) {
                    // update the wrapper with schema updates
                    stream->getWrapper()->update(*stream, *m_database);
                    m_exportingTables[stream->name()] = stream;
                }

                snprintf(msg, sizeof(msg), "Table %s was successfully rebuilt with new schema.",
                        catalogTable->name().c_str());
                msg[sizeof msg - 1] = '\0';
                LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_DEBUG, msg);

                // don't continue on to modify/add/remove indexes, because the
                // call above should rebuild them all anyway
                continue;
            }

            //////////////////////////////////////////
            // find all of the indexes to add
            //////////////////////////////////////////

            auto currentIndexes = persistentTable->allIndexes();
            PersistentTable *deltaTable = persistentTable->deltaTable();

            {
                ConditionalExecuteWithMpMemory useMpMemoryIfReplicated(persistentTable->isReplicatedTable());
                // iterate over indexes for this table in the catalog
                for (LabeledIndex labeledIndex : catalogTable->indexes()) {
                    auto foundIndex = labeledIndex.second;
                    std::string indexName = foundIndex->name();
                    std::string catalogIndexId = TableCatalogDelegate::getIndexIdString(*foundIndex);

                    // Look for an index on the table to match the catalog index
                    bool found = false;
                    for (TableIndex* currIndex : currentIndexes) {
                        const std::string& currentIndexId = currIndex->getId();
                        if (catalogIndexId == currentIndexId) {
                            // rename the index if needed (or even if not)
                            currIndex->rename(indexName);
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        // create and add the index
                        TableIndexScheme scheme;
                        bool success = TableCatalogDelegate::getIndexScheme(*catalogTable,
                                                                            *foundIndex,
                                                                            persistentTable->schema(),
                                                                            &scheme);
                        if (!success) {
                            VOLT_ERROR("Failed to initialize index '%s' from catalog",
                                       foundIndex->name().c_str());
                            return false;
                        }

                        TableIndex *index = TableIndexFactory::getInstance(scheme);
                        vassert(index);
                        VOLT_TRACE("create and add the index for %s", index->getName().c_str());

                        // all of the data should be added here
                        persistentTable->addIndex(index);
                        // Add the same index structure to the delta table.
                        if (deltaTable) {
                            TableIndex *indexForDelta = TableIndexFactory::getInstance(scheme);
                            deltaTable->addIndex(indexForDelta);
                        }

                        // add the index to the stats source
                        index->getIndexStats()->configure(index->getName() + " stats",
                                                          persistentTable->name());
                    }
                }

                //////////////////////////////////////////
                // now find all of the indexes to remove
                //////////////////////////////////////////

                // iterate through all of the existing indexes
                for (TableIndex* currIndex : currentIndexes) {
                    const std::string& currentIndexId = currIndex->getId();

                    bool found = false;
                    // iterate through all of the catalog indexes,
                    //  looking for a match.
                    for (LabeledIndex labeledIndex : catalogTable->indexes()) {
                        std::string catalogIndexId =
                            TableCatalogDelegate::getIndexIdString(*(labeledIndex.second));
                        if (catalogIndexId == currentIndexId) {
                            found = true;
                            break;
                        }
                    }

                    // if the table has an index that the catalog doesn't,
                    // then remove the index
                    if (!found) {
                        persistentTable->removeIndex(currIndex);
                        // Remove the same index structure from the delta table.
                        if (deltaTable) {
                            const std::vector<TableIndex*> currentDeltaIndexes = deltaTable->allIndexes();
                            for (TableIndex* currDeltaIndex : currentDeltaIndexes) {
                                if (currDeltaIndex->getId() == currentIndexId) {
                                    deltaTable->removeIndex(currDeltaIndex);
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            ///////////////////////////////////////////////////
            // now find all of the materialized views to remove
            ///////////////////////////////////////////////////
            rebuildViewsOnTable<PersistentTable, MaterializedViewTriggerForWrite>(catalogTable, persistentTable);
        }
    }

    for (LabeledFunction labeledFunction : m_database->functions()) {
        auto catalogFunction = labeledFunction.second;
        UserDefinedFunctionInfo *info = new UserDefinedFunctionInfo();
        info->returnType = (ValueType) catalogFunction->returnType();
        catalog::CatalogMap<catalog::FunctionParameter> parameters = catalogFunction->parameters();
        info->paramTypes.resize(parameters.size());
        for (auto iter : parameters) {
            int key = std::stoi(iter.first);
            info->paramTypes[key] = (ValueType)iter.second->parameterType();
        }

        VOLT_DEBUG("UDFCAT: Adding function info (ID = %d)", catalogFunction->functionId());
        // If the function info already exists, release the previous info structure.
        if (m_functionInfo.find(catalogFunction->functionId()) != m_functionInfo.end()) {
            VOLT_DEBUG("UDFCAT:    The function info already exists.");
            delete m_functionInfo[catalogFunction->functionId()];
        }
        m_functionInfo[catalogFunction->functionId()] = info;
    }

    // new plan fragments are handled differently.
    return true;
}

void VoltDBEngine::attachTupleStream(
        StreamedTable* streamedTable,
        const std::string& streamName,
        std::map<std::string, ExportTupleStream*> & purgedStreams,
        int64_t generation) {
    m_exportingTables[streamName] = streamedTable;
    ExportTupleStream* wrapper = streamedTable->getWrapper();
    if (wrapper == nullptr) {
        wrapper = purgedStreams[streamName];
        if (wrapper == nullptr) {
            const catalog::Topic* topic = TopicTupleStream::getTopicForStream(*streamedTable, *m_database);
            wrapper = topic == nullptr ?
                    new ExportTupleStream(m_executorContext->m_partitionId, m_executorContext->m_siteId, generation,
                            streamName) :
                    TopicTupleStream::create(*streamedTable, *topic, m_executorContext->m_partitionId,
                            m_executorContext->m_siteId, generation);
            wrapper->extendBufferChain(0);
        } else {
            purgedStreams[streamName] = nullptr;
        }
        streamedTable->setWrapper(wrapper);
        VOLT_TRACE("created stream export wrapper stream %s", streamName.c_str());
    } else {
        // If stream was dropped in UAC and the added back we should not purge the wrapper.
        // A case when exact same stream is dropped and added.
        vassert(purgedStreams[streamName] == NULL);
    }
}

void VoltDBEngine::detachTupleStream(
        StreamedTable* streamedTable,
        const std::string& streamName,
        std::map<std::string, ExportTupleStream*> & purgedStreams) {
    m_exportingTables[streamName] = streamedTable;
    ExportTupleStream* wrapper = streamedTable->getWrapper();
    if (wrapper != nullptr) {
        vassert(purgedStreams[streamName] == NULL);
        purgedStreams[streamName] = wrapper;
        streamedTable->setWrapper(NULL);
        VOLT_TRACE("detached stream export wrapper stream %s", streamName.c_str());
    }
}

/*
 * Accept a list of catalog commands expressing a diff between the
 * current and the desired catalog. Execute those commands and create,
 * delete or modify the corresponding execution engine objects.
 */
bool VoltDBEngine::updateCatalog(int64_t timestamp, bool isStreamUpdate, std::string const& catalogPayload) {
    // clean up execution plans when the tables underneath might change
    if (m_plans) {
        m_plans->clear();
    }

    vassert(m_catalog != NULL); // the engine must be initialized
    VOLT_DEBUG("Updating catalog...");
    // apply the diff commands to the existing catalog
    // throws SerializeEEExceptions on error.
    m_catalog->execute(catalogPayload);

    // Set DR flag based on current catalog state
    auto catalogCluster = m_catalog->clusters().get("cluster");
    m_executorContext->drStream()->m_enabled = catalogCluster->drProducerEnabled();
    assert(!catalogCluster->drProducerEnabled() || catalogCluster->drFlushInterval() > 0);
    m_executorContext->drStream()->setFlushInterval(catalogCluster->drFlushInterval());
    if (m_executorContext->drReplicatedStream()) {
        m_executorContext->drReplicatedStream()->m_enabled = catalogCluster->drProducerEnabled();
        m_executorContext->drReplicatedStream()->setFlushInterval(catalogCluster->drFlushInterval());
    }
    s_exportFlushTimeout = catalogCluster->exportFlushInterval();
    assert(s_exportFlushTimeout > 0);

    if (updateCatalogDatabaseReference() == false) {
        VOLT_ERROR("Error re-caching catalog references.");
        return false;
    }

    std::map<std::string, ExportTupleStream*> purgedStreams;
    processCatalogDeletes(timestamp, false, purgedStreams);
    if (SynchronizedThreadLock::countDownGlobalTxnStartCount(isLowestSite())) {
        processReplicatedCatalogDeletes(timestamp, purgedStreams);
        SynchronizedThreadLock::signalLowestSiteFinished();
    }

    if (processCatalogAdditions(timestamp, false, isStreamUpdate, purgedStreams) == false) {
        VOLT_ERROR("Error processing catalog additions.");
        purgedStreams.clear();
        return false;
    }

    rebuildTableCollections();

    if (SynchronizedThreadLock::countDownGlobalTxnStartCount(isLowestSite())) {
        vassert(SynchronizedThreadLock::isLowestSiteContext());
        VOLT_TRACE("updating catalog from partition %d", m_partitionId);

        // load up all the tables, adding all tables
        if (processReplicatedCatalogAdditions(timestamp, isStreamUpdate, purgedStreams) == false) {
            return false;
        }

        rebuildReplicatedTableCollections();

        initReplicatedMaterializedViewsAndLimitDeletePlans();

        SynchronizedThreadLock::signalLowestSiteFinished();
    }

    initMaterializedViewsAndLimitDeletePlans();

    purgeMissingStreams(purgedStreams);

    // Because Join views of partitioned tables could update the handler list of replicated tables we need to make
    // sure all partitions finish these updates before allowing other transactions to touch the replicated tables
    if (SynchronizedThreadLock::countDownGlobalTxnStartCount(isLowestSite())) {
        SynchronizedThreadLock::signalLowestSiteFinished();
    }

    m_catalog->purgeDeletions();

    VOLT_DEBUG("Updated catalog...");
    return true;
}

void
VoltDBEngine::purgeMissingStreams(std::map<std::string, ExportTupleStream*> & purgedStreams) {
    for (LabeledStreamWrapper entry : purgedStreams) {
        if (entry.second) {
            // purgedStreams should have been flushed by quiesce
            assert(!entry.second->testFlushPending());
            delete entry.second;
        }
    }
}

bool VoltDBEngine::loadTable(int32_t tableId, ReferenceSerializeInputBE &serializeIn,
        int64_t txnId, int64_t spHandle, int64_t lastCommittedSpHandle,
        int64_t uniqueId, int64_t undoToken, const LoadTableCaller &caller) {
    //Not going to thread the unique id through.
    //The spHandle and lastCommittedSpHandle aren't really used in load table
    //since their only purpose as of writing this (1/2013) they are only used
    //for export data and we don't technically support loading into an export table
    setUndoToken(undoToken);
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(), txnId, spHandle,
            lastCommittedSpHandle, uniqueId, false);

    if (caller.shouldDrStream()) {
        m_executorContext->checkTransactionForDR();
    }

    Table* ret = getTableById(tableId);
    if (ret == nullptr) {
        VOLT_ERROR("Table ID %d doesn't exist. Could not load data", (int) tableId);
        return false;
    }

    PersistentTable* table = dynamic_cast<PersistentTable*>(ret);
    if (table == nullptr) {
        VOLT_ERROR("Table ID %d(name '%s') is not a persistent table. Could not load data",
                   (int) tableId, ret->name().c_str());
        return false;
    }

    // If this is a paused non-empty single table view, load data into the delta table
    // instead of the main table.
    if (table->deltaTable() && table->materializedViewTrigger()) {
        table = table->deltaTable();
    }

    // When loading a replicated table, behavior should be:
    //   ConstraintFailureException may be thrown on the lowest site thread.
    //   If returnConflictRows is false:
    //       Lowest site thread: throw the exception.
    //       Other site threads: throw replicated table exceptions.
    //   else (returnConflictRows is true)
    //       Lowest site thread: serialize the offending rows, return 1.
    //       Other site thread: copy the serialized buffer from lowest site, return 1.
    //
    //   SQLException may also be thrown on the loweset site thread (e.g. from the TableTuple.deserializeFrom())
    //   will always re/throw from every sites
    //
    //   For all other kinds of exceptions, throw a FatalException.  This is legacy behavior.
    //   Perhaps we cannot be ensured of data integrity for other kinds of exceptions?

    ConditionalSynchronizedExecuteWithMpMemory possiblySynchronizedUseMpMemory
            (table->isReplicatedTable(), isLowestSite(), []() {
             s_loadTableException = VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE;
             });
    if (possiblySynchronizedUseMpMemory.okToExecute()) {
        // Joined views are special. If any of the source table(s) are not empty, we cannot restore the view content
        // from a snapshot. The Java top-end has no way to know this so it still tries to tell the EE
        // to pause the view on a snapshot restore. When EE finds out that any of the source tables are not empty, it
        // will ignore the request and let the view stay active. In this case, loadTable() should no longer import
        // the data from the snapshot to the view table. - ENG-15918
        auto handler = table->materializedViewHandler();
        if (handler && handler->snapshotable() && handler->isEnabled()) {
            char msg[256];
            snprintf(msg, sizeof(msg),
                    "Materialized view %s joining multiple tables was skipped in the snapshot restore because it is not paused.",
                    table->name().c_str());
            msg[sizeof msg - 1] = '\0';
            LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_INFO, msg);
            return true;
        }
        try {
            table->loadTuplesForLoadTable(serializeIn, NULL,
                    caller.returnConflictRows() ? &m_resultOutput : NULL, caller);
        } catch (const ConstraintFailureException &cfe) {
            s_loadTableException = VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_CONSTRAINT_VIOLATION;
            if (caller.returnConflictRows()) {
                // This should not happen because all errors are swallowed and constraint violations are returned
                // as failed rows in the result
                throw;
            } else {
                // pre-serialize the exception here since we need to cleanup tuple memory within this sync block
                resetReusedResultOutputBuffer();
                cfe.serialize(getExceptionOutputSerializer());
                return false;
            }
        } catch (const SQLException &sqe) {
            s_loadTableException = VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_SQL;
            throw;
        } catch (const SerializableEEException& serializableExc) {
            // Exceptions that are not constraint failures or sql exeception are treated as fatal.
            // This is legacy behavior.  Perhaps we cannot be ensured of data integrity for some mysterious
            // other kind of exception?
            s_loadTableException = serializableExc.getType();
            throwFatalException("%s", serializableExc.message().c_str());
        }

        if (table->isReplicatedTable() && caller.returnConflictRows()) {
            // There may or may not have been conflicts but the call always succeeds. We need to copy the
            // lowest site result into the results of other sites so there are no hash mismatches.
            ExecuteWithAllSitesMemory execAllSites;
            for (auto engineIt = execAllSites.begin(); engineIt != execAllSites.end(); ++engineIt) {
                EngineLocals &curr = engineIt->second;
                VoltDBEngine *currEngine = curr.context->getContextEngine();
                currEngine->m_resultOutput.writeBytes(m_resultOutput.data(), m_resultOutput.size());
            }
        }

        // Indicate to other threads that load happened successfully.
        s_loadTableException = VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_NONE;
    } else if (s_loadTableException == VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_CONSTRAINT_VIOLATION) {
        // An constraint failure exception was thrown on the lowest site thread and
        // handle it on the other threads too.
        if (!caller.returnConflictRows()) {
            std::ostringstream oss;
            oss << "Replicated load table failed (constraint violation) on other thread for table \""
                << table->name() << "\".\n";
            VOLT_DEBUG("%s", oss.str().c_str());
            throw SerializableEEException(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE,
                    oss.str().c_str());
        }
        // Offending rows will be serialized on lowest site thread.
        return false;
    } else if (s_loadTableException == VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_SQL) {
        // An sql exception was thrown on the lowest site thread and
        // handle it on the other threads too.
        std::ostringstream oss;
        oss << "Replicated load table failed (sql exception) on other thread for table \""
            << table->name() << "\".\n";
        VOLT_DEBUG("%s", oss.str().c_str());
        throw SerializableEEException(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE,
                oss.str().c_str());
    } else if (s_loadTableException != VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_NONE) {
        // some other kind of exception occurred on lowest site thread
        // This is fatal.
        std::ostringstream oss;
        oss << "An unknown exception occurred on another thread when loading table \"" << table->name() << "\".";
        VOLT_DEBUG("%s", oss.str().c_str());
        throwFatalException("%s", oss.str().c_str());
    }
    return true;
}

/*
 * Delete and rebuild the relativeIndex based table collections.
 * It does not affect any currently stored tuples.
 */
void VoltDBEngine::rebuildTableCollections(bool updateReplicated, bool fromScratch) {
    VOLT_DEBUG("UpdateReplicated = %s,%s from scratch",
               updateReplicated ? "true" : "false",
               fromScratch ? "" : " not");
    // 1. See header comments explaining m_snapshottingTables.
    // 2. Don't clear m_exportTables. They are still exporting, even if deleted.
    // 3. Clear everything else.
    if (! updateReplicated && fromScratch) {
        m_tables.clear();
        m_tablesByName.clear();
        m_replicableTables.clear();

        // need to re-map all the table ids / indexes
        getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_TABLE);
        getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_INDEX);

        for (auto& entry : m_systemTables) {
            m_tables[static_cast<CatalogId>(entry.first)] = entry.second;
        }
    }

    // Walk through table delegates and update local table collections
    for (LabeledTCD cd : m_catalogDelegates) {
        auto tcd = cd.second;
        vassert(tcd);
        if (! tcd) {
            continue;
        }
        Table* localTable = tcd->getTable();
        vassert(localTable);
        if (! localTable) {
            VOLT_ERROR("DEBUG-NULL: %s", cd.first.c_str());
            continue;
        }
        vassert(m_database);
        auto catTable = m_database->tables().get(localTable->name());
        int32_t relativeIndexOfTable = catTable->relativeIndex();
        const std::string& tableName = tcd->getTable()->name();
        if (catTable->isreplicated()) {
            if (updateReplicated) {
                // This engine is responsible for updating the catalog table maps for all sites.
                ExecuteWithAllSitesMemory execAllSites;
                for (auto engineIt : execAllSites) {
                    EngineLocals& curr = engineIt.second;
                    VoltDBEngine* currEngine = curr.context->getContextEngine();
                    SynchronizedThreadLock::assumeSpecificSiteContext(curr);
                    currEngine->m_tables[relativeIndexOfTable] = localTable;
                    currEngine->m_tablesByName[tableName] = localTable;
                }
            }
        } else if (! updateReplicated) {
            m_tables[relativeIndexOfTable] = localTable;
            m_tablesByName[tableName] = localTable;
        }

        TableStats* stats = NULL;
        PersistentTable* persistentTable = tcd->getPersistentTable();
        if (persistentTable) {
            stats = persistentTable->getTableStats();

            if (!fromScratch && persistentTable->isDREnabled() && !tcd->materialized()) {
                // If the tables aren't from scratch make sure to update any replicable table pointers
                // When from scratch the caller should set the pointers by calling setReplicableTables()
                int64_t hash = tcd->signatureHashAsLong();
                if (catTable->isreplicated()) {
                    if (updateReplicated) {
                        ExecuteWithAllSitesMemory execAllSites;
                        for (auto engineIt : execAllSites) {
                            EngineLocals& curr = engineIt.second;
                            VoltDBEngine* currEngine = curr.context->getContextEngine();
                            SynchronizedThreadLock::assumeSpecificSiteContext(curr);
                            currEngine->updateReplicableTablePointer(hash, persistentTable);
                        }
                    }
                } else if (!updateReplicated) {
                    updateReplicableTablePointer(hash, persistentTable);
                }
            }

            // add all of the indexes to the stats source
            std::vector<TableIndex*> const& tindexes = persistentTable->allIndexes();
            if (catTable->isreplicated()) {
                if (updateReplicated) {
                    ExecuteWithAllSitesMemory execAllSites;
                    for (auto engineIt : execAllSites) {
                        EngineLocals& curr = engineIt.second;
                        VoltDBEngine* currEngine = curr.context->getContextEngine();
                        SynchronizedThreadLock::assumeSpecificSiteContext(curr);
                        if (! fromScratch) {
                            // This is a swap or truncate and we need to clear the old index stats sources for this table
                            currEngine->getStatsManager().unregisterStatsSource(
                                    STATISTICS_SELECTOR_TYPE_TABLE, relativeIndexOfTable);
                            currEngine->getStatsManager().unregisterStatsSource(
                                    STATISTICS_SELECTOR_TYPE_INDEX, relativeIndexOfTable);
                        }
                        for (auto index : tindexes) {
                            currEngine->getStatsManager().registerStatsSource(
                                    STATISTICS_SELECTOR_TYPE_INDEX, relativeIndexOfTable, index->getIndexStats());
                        }
                        currEngine->getStatsManager().registerStatsSource(
                                STATISTICS_SELECTOR_TYPE_TABLE, relativeIndexOfTable, stats);
                    }
                }
            } else if (! updateReplicated) {
                if (! fromScratch) {
                    // This is a swap or truncate and we need to clear the old index stats sources for this table
                    getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_TABLE, relativeIndexOfTable);
                    getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_INDEX, relativeIndexOfTable);
                }
                for (auto index : tindexes) {
                    getStatsManager().registerStatsSource(STATISTICS_SELECTOR_TYPE_INDEX,
                            relativeIndexOfTable, index->getIndexStats());
                }
                getStatsManager().registerStatsSource(STATISTICS_SELECTOR_TYPE_TABLE,
                        relativeIndexOfTable, stats);
            }
        } else {
            // Streamed tables are all partitioned.
            if (updateReplicated) {
                continue;
            }
            // Streamed tables could not be truncated or swapped, so we will never change this
            // table stats map in a not-from-scratch mode.
            // Before this rebuildTableCollections() is called, pre-built DR conflict tables
            // (which are also streamed tables) should have already been instantiated.
            else if (fromScratch) {
                stats = tcd->getStreamedTable()->getTableStats();
                getStatsManager().registerStatsSource(STATISTICS_SELECTOR_TYPE_TABLE,
                        relativeIndexOfTable, stats);
            }
        }
    }

    resetDRConflictStreamedTables();
}

inline void VoltDBEngine::updateReplicableTablePointer(int64_t hash, PersistentTable* table) {
    for (auto& replicableTables : m_replicableTables) {
         auto tableHash = replicableTables.second.find(hash);
         if (tableHash != replicableTables.second.end()) {
             tableHash->second = table;
         }
     }
}

void VoltDBEngine::swapDRActions(PersistentTable* table1, PersistentTable* table2) {
    TableCatalogDelegate* tcd1 = getTableDelegate(table1->name());
    TableCatalogDelegate* tcd2 = getTableDelegate(table2->name());
    vassert(!tcd1->materialized());
    vassert(!tcd2->materialized());
    // Point the Map from signature hash point to the correct persistent tables
    int64_t hash1 = tcd1->signatureHashAsLong();
    int64_t hash2 = tcd2->signatureHashAsLong();
    // Most swap action is already done.
    // But hash(tcd1) is still pointing to old persistent table1, which is now table2.
    for (auto& entry : m_replicableTables) {
        auto& hashMap = entry.second;
        auto hashEntry = hashMap.find(hash1);
        if (hashEntry != hashMap.end()) {
            // Table2 was in the map so update the pointer to table1
            vassert(hashEntry->second == table2);
            hashEntry->second = table1;
        }

        hashEntry = hashMap.find(hash2);
        if (hashEntry != hashMap.end()) {
            // Table1 was in the map so update the pointer to table2
            vassert(hashEntry->second == table1);
            hashEntry->second = table2;
        }
    }
    table1->signature(tcd1->signatureHash());
    table2->signature(tcd2->signatureHash());

    // Generate swap table DREvent
    vassert(table1->isDREnabled() == table2->isDREnabled());
    vassert(table1->isDREnabled()); // This is checked before calling this method.
    int64_t lastCommittedSpHandle = m_executorContext->lastCommittedSpHandle();
    int64_t spHandle = m_executorContext->currentSpHandle();
    int64_t uniqueId = m_executorContext->currentUniqueId();
    // Following are stored: name1.length, name1, name2.length, name2, hash1, hash2
    int32_t bufferLength = 4 + table1->name().length() + 4 + table2->name().length() + 8 + 8;
    char* payloadBuffer[bufferLength];
    ExportSerializeOutput io(payloadBuffer, bufferLength);
    io.writeBinaryString(table1->name().c_str(), table1->name().length());
    io.writeBinaryString(table2->name().c_str(), table2->name().length());
    io.writeLong(hash1);
    io.writeLong(hash2);
    ByteArray payload(io.data(), io.size());

    quiesce(lastCommittedSpHandle);
    if (m_executorContext->drStream()->drStreamStarted()) {
        m_executorContext->drStream()->generateDREvent(SWAP_TABLE, spHandle, uniqueId, payload);
    }
    if (m_executorContext->drReplicatedStream() &&
            m_executorContext->drReplicatedStream()->drStreamStarted()) {
        m_executorContext->drReplicatedStream()->generateDREvent(
                SWAP_TABLE, spHandle, uniqueId, payload);
    }
}

void VoltDBEngine::resetDRConflictStreamedTables() {
    if (getIsActiveActiveDREnabled()) {
        // These tables that go by well-known names SHOULD exist in an active-active
        // catalog except perhaps in some test scenarios with specialized (stubbed)
        // VoltDBEngine implementations, so avoid asserting that they exist.
        // DO assert that if the well-known streamed table exists, it has the right type.
        Table* wellKnownTable = getTableByName(DR_PARTITIONED_CONFLICT_TABLE_NAME);
        m_drPartitionedConflictStreamedTable =
            dynamic_cast<StreamedTable*>(wellKnownTable);
        vassert(m_drPartitionedConflictStreamedTable == wellKnownTable);
        wellKnownTable = getTableByName(DR_REPLICATED_CONFLICT_TABLE_NAME);
        m_drReplicatedConflictStreamedTable =
            dynamic_cast<StreamedTable*>(wellKnownTable);
        vassert(m_drReplicatedConflictStreamedTable == wellKnownTable);
    } else {
        m_drPartitionedConflictStreamedTable = NULL;
        m_drReplicatedConflictStreamedTable = NULL;
    }
}

void VoltDBEngine::setExecutorVectorForFragmentId(int64_t fragId) {
    if (m_plans) {
        PlanSet& existing_plans = *m_plans;
        PlanSet::nth_index<1>::type::iterator iter = existing_plans.get<1>().find(fragId);

        // found it, move it to the front
        if (iter != existing_plans.get<1>().end()) {
            // move it to the front of the list
            PlanSet::iterator iter2 = existing_plans.project<0>(iter);
            existing_plans.get<0>().relocate(existing_plans.begin(), iter2);
            m_currExecutorVec = (*iter).get();
            // update the context
            m_currExecutorVec->setupContext(m_executorContext);
            return;
        }
    } else {
        m_plans.reset(new EnginePlanSet());
    }

    PlanSet& plans = *m_plans;
    std::string plan = m_topend->planForFragmentId(fragId);
    if (plan.empty()) {
        throwSerializableEEException(
                "Fetched empty plan from frontend for PlanFragment '%jd'", (intmax_t)fragId);
    }

    boost::shared_ptr<ExecutorVector> ev_guard = ExecutorVector::fromJsonPlan(this, plan, fragId);

    // add the plan to the back
    //
    // (Why to the back?  Shouldn't it be at the front with the
    // most recently used items?  See ENG-7244)
    plans.get<0>().push_back(ev_guard);

    // remove a plan from the front if the cache is full
    if (plans.size() > PLAN_CACHE_SIZE) {
        PlanSet::iterator iter = plans.get<0>().begin();
        plans.erase(iter);
    }

    m_currExecutorVec = ev_guard.get();
    vassert(m_currExecutorVec);
}

// -------------------------------------------------
// Initialization Functions
// -------------------------------------------------
// Parameter MATVIEW is the materialized view class,
// either MaterializedViewTriggerForStreamInsert for views on streams or
// MaterializedViewTriggerForWrite for views on persistent tables.
template <class MATVIEW>
static bool updateMaterializedViewDestTable(std::vector<MATVIEW*> & views,
        PersistentTable* target, catalog::MaterializedViewInfo* targetMvInfo) {
    const std::string& targetName = target->name();

    // find the materialized view that uses the table or its precursor (by the same name).
    for(MATVIEW* currView : views) {
        PersistentTable* currTarget = currView->destTable();
        const std::string& currName = currTarget->name();
        if (currName != targetName) {
            continue;
        }
        // Found the current view whose target table has the matching name.
        // Update it as needed to the latest target table and view definition.
        currView->updateDefinition(target, targetMvInfo);
        return true;
    }
    return false;
}


// Parameter TABLE is the table class for the source table on which
// a view can be defined, StreamedTable or PersistentTable.
// Currently, these correspond one to one with MATVIEW types via
// their MatViewType member typedefs, but this may need to change
// in the future if there are different view types with different
// triggered behavior defined on the same class of table.
template<class TABLE> void VoltDBEngine::initMaterializedViews(catalog::Table* catalogTable,
        TABLE* storageTable, bool updateReplicated) {
    // walk views
    VOLT_DEBUG("Processing views for table %s", storageTable->name().c_str());
    BOOST_FOREACH (LabeledView labeledView, catalogTable->views()) {
        auto catalogView = labeledView.second;
        catalog::Table const* destCatalogTable = catalogView->dest();
        int32_t catalogIndex = destCatalogTable->relativeIndex();
        auto destTable = static_cast<PersistentTable*>(m_tables[catalogIndex]);
        vassert(destTable);
        VOLT_DEBUG("Updating view on table %s", destTable->name().c_str());
        vassert(destTable == dynamic_cast<PersistentTable*>(m_tables[catalogIndex]));
        // Ensure that the materialized view controlling the existing
        // target table by the same name is using the latest version of
        // the table and view definition.
        // OR create a new materialized view link to connect the tables
        // if there is not one already with a matching target table name.
        ConditionalExecuteWithMpMemory useMpMemoryIfReplicated(updateReplicated);
        if ( ! updateMaterializedViewDestTable(
                    storageTable->views(), destTable, catalogView)) {
            // This is a new view, a connection needs to be made using a new MaterializedViewTrigger..
            TABLE::MatViewType::build(storageTable, destTable, catalogView);
        }
        VOLT_DEBUG("Finished update for view on table %s", destTable->name().c_str());
    }

    catalog::MaterializedViewHandlerInfo *mvHandlerInfo = catalogTable->mvHandlerInfo().get("mvHandlerInfo");
    // If the table has a mvHandlerInfo, it means this table is a target table of materialized view.
    // Now we only use the new view handler mechanism for joined table views.
    // Further code refactoring saved for future.
    if (mvHandlerInfo) {
        auto destTable = static_cast<PersistentTable*>(m_tables[catalogTable->relativeIndex()]);
        if ( ! destTable->materializedViewHandler() || destTable->materializedViewHandler()->isDirty() ) {
            // The newly-added handler will at the same time trigger
            // the uninstallation of the previous (if exists) handler.
            VOLT_DEBUG("Creating view handler for table %s", destTable->name().c_str());
            auto handler = new MaterializedViewHandler(destTable, mvHandlerInfo,
                    mvHandlerInfo->groupByColumnCount(), this);
            if (destTable->isPersistentTableEmpty()) {
                handler->catchUpWithExistingData(false);
            }
        }
    }
}

/*
 * Iterate catalog tables looking for tables that are materialized
 * view sources.  When found, construct a materialized view metadata
 * object that connects the source and destination tables, and assign
 * that object to the source table.
 *
 * Assumes all tables (sources and destinations) have been constructed.
 */
void VoltDBEngine::initMaterializedViewsAndLimitDeletePlans(bool updateReplicated) {
    // walk tables
    BOOST_FOREACH (LabeledTable labeledTable, m_database->tables()) {
        auto catalogTable = labeledTable.second;
        // When updateReplicated flag is set, only replicated table work allowed, and vice versa.
        if (catalogTable->isreplicated() != updateReplicated) {
            continue;
        }
        Table *table = m_tables[catalogTable->relativeIndex()];
        PersistentTable *persistentTable = dynamic_cast<PersistentTable*>(table);
        if (persistentTable != NULL) {
            initMaterializedViews(catalogTable, persistentTable, updateReplicated);
        } else {
            auto streamedTable = dynamic_cast<StreamedTable*>(table);
            vassert(streamedTable);
            initMaterializedViews(catalogTable, streamedTable, updateReplicated);
        }
    }
}


const unsigned char* VoltDBEngine::getResultsBuffer() const {
    return (const unsigned char*)m_resultOutput.data();
}

int VoltDBEngine::getResultsSize() const {
    return static_cast<int>(m_resultOutput.size());
}

int32_t VoltDBEngine::getPerFragmentStatsSize() const {
    return static_cast<int32_t>(m_perFragmentStatsOutput.size());
}

void VoltDBEngine::setBuffers(
            char* parameterBuffer,        int parameterBufferCapacity,
            char* perFragmentStatsBuffer, int perFragmentStatsBufferCapacity,
            char* udfBuffer,              int udfBufferCapacity,
            char* firstResultBuffer,      int firstResultBufferCapacity,
            char* nextResultBuffer,       int nextResultBufferCapacity,
            char* exceptionBuffer,        int exceptionBufferCapacity) {
    m_parameterBuffer = parameterBuffer;
    m_parameterBufferCapacity = parameterBufferCapacity;

    m_perFragmentStatsBuffer = perFragmentStatsBuffer;
    m_perFragmentStatsBufferCapacity = perFragmentStatsBufferCapacity;

    m_udfBuffer = udfBuffer;
    m_udfBufferCapacity = udfBufferCapacity;

    m_firstReusedResultBuffer = firstResultBuffer;
    m_firstReusedResultCapacity = firstResultBufferCapacity;

    m_nextReusedResultBuffer = nextResultBuffer;
    m_nextReusedResultCapacity = nextResultBufferCapacity;

    m_exceptionBuffer = exceptionBuffer;
    m_exceptionBufferCapacity = exceptionBufferCapacity;
}

// -------------------------------------------------
// MISC FUNCTIONS
// -------------------------------------------------

bool VoltDBEngine::isLocalSite(const NValue& value) const {
    int32_t index = m_hashinator->hashinate(value);
    return index == m_partitionId;
}

int32_t VoltDBEngine::getPartitionForPkHash(const int32_t pkHash) const {
    return m_hashinator->partitionForToken(pkHash);
}

bool VoltDBEngine::isLocalSite(const int32_t pkHash) const {
    return getPartitionForPkHash(pkHash) == m_partitionId;
}

std::string VoltDBEngine::dumpCurrentHashinator() const {
    return m_hashinator.get()->debug();
}

void VoltDBEngine::setStreamFlushTarget(int64_t targetTime, StreamedTable* table) {

}


/** Perform once per second, non-transactional work. */
void VoltDBEngine::tick(int64_t timeInMillis, int64_t lastCommittedSpHandle) {
    #if !defined(NDEBUG) && defined(MACOSX)
    // When debugging the VoltDB execution engine started via JNI with LLDB,
    // It is very common to run into an EXC_BAD_ACCESS in the debugger and cannot
    // get out. LLDB uses task_set_exception_ports() to change the exception port of
    // the process it is debugging. Here, we call the same function again to override
    // the exception port for EXC_BAD_ACCESS, so that it won't be passed to LLDB.
    // int mute_exc_bad_access_for_debugging =
    task_set_exception_ports(mach_task_self(), EXC_MASK_BAD_ACCESS, MACH_PORT_NULL, EXCEPTION_DEFAULT, 0);
    #endif
    m_executorContext->setupForTick(lastCommittedSpHandle);
    //Push tuples for exporting streams.
    ExportTupleStream* oldestUnflushed = NULL;
    ExportTupleStream* newestUnflushed = NULL;
    ExportTupleStream* nextStreamToFlush = m_oldestExportStreamWithPendingRows;
    while (nextStreamToFlush) {
        // While flush interval is global, we can stop the list processing when the flush timeout
        // is exceeded
        if (nextStreamToFlush->flushTimerExpired(timeInMillis)) {
            // Get next stream before the flush because a successful flush sets next and prev to NULL
            nextStreamToFlush = nextStreamToFlush->getNextFlushStream();
            if (!m_oldestExportStreamWithPendingRows->periodicFlush(timeInMillis, lastCommittedSpHandle)) {
                // If periodicFlush returns false, it means there is an MP txn in progress that prevented the flush
                m_oldestExportStreamWithPendingRows->resetFlushLinkages();
                m_oldestExportStreamWithPendingRows->appendToList(&oldestUnflushed, &newestUnflushed);
            }
            m_oldestExportStreamWithPendingRows = nextStreamToFlush;
        } else {
            // We tried to flush a stream that has not yet hit it's flush timer,
            // this stream is now at the head of the list.
            nextStreamToFlush->setPrevFlushStream(NULL);
            break;
        }
    }
    if (newestUnflushed) {
        if (nextStreamToFlush) {
            // We stopped flushing in the middle of the list. If there are any skipped streams stitch them together
            vassert(m_oldestExportStreamWithPendingRows);
            newestUnflushed->stitchToNextNode(m_oldestExportStreamWithPendingRows);
        } else {
            // We went through the whole list but skipped streams in the middle of an MP.
            m_newestExportStreamWithPendingRows = newestUnflushed;
        }
        m_oldestExportStreamWithPendingRows = oldestUnflushed;
    } else if (m_oldestExportStreamWithPendingRows == NULL) {
        m_newestExportStreamWithPendingRows = NULL;
    }

    m_executorContext->drStream()->periodicFlush(timeInMillis, lastCommittedSpHandle);
    if (m_executorContext->drReplicatedStream()) {
        m_executorContext->drReplicatedStream()->periodicFlush(timeInMillis, lastCommittedSpHandle);
    }
}

/** Bring the Export and DR system to a steady state with no pending committed data */
void VoltDBEngine::quiesce(int64_t lastCommittedSpHandle) {
    m_executorContext->setupForQuiesce(lastCommittedSpHandle);
    for (auto const &streamTable : m_exportingTables) {
        if (streamTable.second->getWrapper()) {
            // A quiesce should be transactional so periodicFlush should always succeed
            __attribute__((unused)) bool streamFlushed =
                streamTable.second->getWrapper()->periodicFlush(-1, lastCommittedSpHandle);
            vassert(streamFlushed);
        }
    }
    m_oldestExportStreamWithPendingRows = NULL;
    m_newestExportStreamWithPendingRows = NULL;

    m_executorContext->drStream()->periodicFlush(-1L, lastCommittedSpHandle);
    if (m_executorContext->drReplicatedStream()) {
        m_executorContext->drReplicatedStream()->periodicFlush(-1L, lastCommittedSpHandle);
    }
}

std::string VoltDBEngine::debug(void) const {
    if ( ! m_plans) {
        return "";
    }
    PlanSet& plans = *m_plans;
    std::ostringstream output;

    for (boost::shared_ptr<ExecutorVector> ev_guard : plans) {
        ev_guard->debug();
    }

    return output.str();
}

/**
 * Retrieve a set of statistics and place them into the result buffer as a set
 * of VoltTables.
 *
 * @param selector StatisticsSelectorType indicating what set of statistics
 *                 should be retrieved
 * @param locators Integer identifiers specifying what subset of possible
 *                 statistical sources should be polled. Probably a CatalogId
 *                 Can be NULL in which case all possible sources for the
 *                 selector should be included.
 * @param numLocators Size of locators array.
 * @param interval Whether to return counters since the beginning or since the
 *                 last time this was called
 * @param Timestamp to embed in each row
 * @return Number of result tables, 0 on no results, -1 on failure.
 */
int VoltDBEngine::getStats(int selector, int locators[], int numLocators,
                           bool interval, int64_t now) {
    Table* resultTable = NULL;
    std::vector<CatalogId> locatorIds;

    for (int ii = 0; ii < numLocators; ii++) {
        CatalogId locator = static_cast<CatalogId>(locators[ii]);
        Table* t = getTableById(locator);
        if (!t) {
            throwSerializableEEException(
                    "getStats() called with selector %d, and an invalid locator %d that does not correspond to a table",
                    selector, locator);
        }
        auto streamTable = dynamic_cast<StreamedTable*>(t);
        if (streamTable == NULL || streamTable->getWrapper() == NULL) {
            // skip stats for stream tables with ExportTupleStreams
            locatorIds.push_back(locator);
        }
    }
    size_t lengthPosition = m_resultOutput.reserveBytes(sizeof(int32_t));

    try {
        switch (selector) {
            case STATISTICS_SELECTOR_TYPE_TABLE:
                resultTable = m_statsManager.getStats(
                        (StatisticsSelectorType) selector,
                        m_siteId, m_partitionId,
                        locatorIds, interval, now);
                break;
            case STATISTICS_SELECTOR_TYPE_INDEX:
                resultTable = m_statsManager.getStats(
                        (StatisticsSelectorType) selector,
                        m_siteId, m_partitionId,
                        locatorIds, interval, now);
                break;
            default:
                throwSerializableEEException(
                        "getStats() called with an unrecognized selector %d", selector);
        }
    } catch (const SerializableEEException &e) {
        serializeException(e);
        return -1;
    }

    if (resultTable != NULL) {
        resultTable->serializeTo(m_resultOutput);
        m_resultOutput.writeIntAt(lengthPosition,
                static_cast<int32_t>(m_resultOutput.size() - sizeof(int32_t)));
        return 1;
    } else {
        return 0;
    }
}


void VoltDBEngine::setCurrentUndoQuantum(voltdb::UndoQuantum* undoQuantum) {
    m_currentUndoQuantum = undoQuantum;
    m_executorContext->setupForPlanFragments(m_currentUndoQuantum);
}


/*
 * Exists to transition pre-existing unit test cases.
 */
void VoltDBEngine::updateExecutorContextUndoQuantumForTest() {
    m_executorContext->setupForPlanFragments(m_currentUndoQuantum);
}

int VoltDBEngine::getSnapshotSchema(const CatalogId tableId, HiddenColumnFilter::Type hiddenColumnFilterType,
        bool forceLive) {
    PersistentTable *table;
    if (forceLive || m_snapshottingTables.empty()) {
        // forced to get live schema or called prior to snapshot being activated
        Table* found = getTableById(tableId);

         if (!found) {
             return -1;
         }

         table = dynamic_cast<PersistentTable*>(found);
         if (table == NULL) {
             vassert(table != NULL);
             return -1;
         }
    } else {
        // Called after snapshot has been activated
        table = findInMapOrNull(tableId, m_snapshottingTables);

        if (table == NULL) {
            return -1;
        }
    }

    resetReusedResultOutputBuffer();

    size_t sizeOffset = m_resultOutput.reserveBytes(sizeof(int32_t));

    table->serializeColumnHeaderTo(m_resultOutput, hiddenColumnFilterType);
    m_resultOutput.writeIntAt(sizeOffset, m_resultOutput.position() - (sizeOffset + sizeof(int32_t)));
    m_resultOutput.writeInt(table->partitionColumn());
    return 0;
}

/**
 * Activate a table stream for the specified table
 * Serialized data:
 *  int: predicate count
 *  string: predicate #1
 *  string: predicate #2
 *  ...
 */
bool VoltDBEngine::activateTableStream(
        const CatalogId tableId,
        TableStreamType streamType,
        HiddenColumnFilter::Type hiddenColumnFilter,
        int64_t undoToken,
        ReferenceSerializeInputBE &serializeIn) {
    Table* found = getTableById(tableId);
    if (! found) {
        return false;
    }

    auto table = dynamic_cast<PersistentTable*>(found);
    if (table == NULL) {
        vassert(table != NULL);
        return false;
    }
    setUndoToken(undoToken);

    // Crank up the necessary persistent table streaming mechanism(s).
    if (!table->activateStream(streamType, hiddenColumnFilter,
                m_partitionId, tableId, serializeIn)) {
        return false;
    }

    // keep track of snapshotting tables. a table already in cow mode
    // can not be re-activated for cow mode.
    if (tableStreamTypeIsSnapshot(streamType)) {
        if (m_snapshottingTables.find(tableId) != m_snapshottingTables.end()) {
            vassert(false);
            return false;
        }

        table->incrementRefcount();
        m_snapshottingTables[tableId] = table;
    }

    return true;
}

/**
 * Serialize tuples to output streams from a table in COW mode.
 * Overload that serializes a stream position array.
 * Return remaining tuple count, 0 if done, or TABLE_STREAM_SERIALIZATION_ERROR on error.
 */
int64_t VoltDBEngine::tableStreamSerializeMore(const CatalogId tableId,
        const TableStreamType streamType, ReferenceSerializeInputBE &serialInput) {
    int64_t remaining = TABLE_STREAM_SERIALIZATION_ERROR;
    try {
        std::vector<int> positions;
        remaining = tableStreamSerializeMore(tableId, streamType, serialInput, positions);
        if (remaining >= 0) {
            char *resultBuffer = getReusedResultBuffer();
            vassert(resultBuffer != NULL);
            int resultBufferCapacity = getReusedResultBufferCapacity();
            if (resultBufferCapacity < sizeof(jint) * positions.size()) {
                throwFatalException("tableStreamSerializeMore: result buffer not large enough");
            }
            ReferenceSerializeOutput results(resultBuffer, resultBufferCapacity);
            // Write the array size as a regular integer.
            vassert(positions.size() <= std::numeric_limits<int32_t>::max());
            results.writeInt((int32_t)positions.size());
            // Copy the position vector's contiguous storage to the returned results buffer.
            for (int ipos : positions) {
                results.writeInt(ipos);
            }
        }
        VOLT_DEBUG("tableStreamSerializeMore: deserialized %d buffers, %ld remaining",
                   (int)positions.size(), (long)remaining);
    } catch (SerializableEEException &e) {
        serializeException(e);
        remaining = TABLE_STREAM_SERIALIZATION_ERROR;
    }

    return remaining;
}

/**
 * Serialize tuples to output streams from a table in COW mode.
 * Overload that populates a position vector provided by the caller.
 * Return remaining tuple count, 0 if done, or TABLE_STREAM_SERIALIZATION_ERROR on error.
 */
int64_t VoltDBEngine::tableStreamSerializeMore(
        const CatalogId tableId,
        const TableStreamType streamType,
        ReferenceSerializeInputBE &serializeIn,
        std::vector<int> &retPositions) {
    // Deserialize the output buffer ptr/offset/length values into a COWStreamProcessor.
    int nBuffers = serializeIn.readInt();
    if (nBuffers <= 0) {
        throwFatalException(
                "Expected at least one output stream in tableStreamSerializeMore(), received %d",
                nBuffers);
    }

    if (!tableStreamTypeIsValid(streamType)) { // Failure
        return TABLE_STREAM_SERIALIZATION_ERROR;
    }

    TupleOutputStreamProcessor outputStreams(nBuffers);
    for (int iBuffer = 0; iBuffer < nBuffers; iBuffer++) {
        char *ptr = reinterpret_cast<char*>(serializeIn.readLong());
        int offset = serializeIn.readInt();
        int length = serializeIn.readInt();
        outputStreams.add(ptr + offset, length);
    }
    retPositions.reserve(nBuffers);

    // Find the table based on what kind of stream we have.
    // If a completed table is polled, return remaining==-1. The
    // Java engine will always poll a fully serialized table one more
    // time (it doesn't see the hasMore return code).
    int64_t remaining = TABLE_STREAM_SERIALIZATION_ERROR;
    PersistentTable *table = nullptr;

    if (tableStreamTypeIsSnapshot(streamType)) {
        // If a completed table is polled, return 0 bytes serialized. The
        // Java engine will always poll a fully serialized table one more
        // time (it doesn't see the hasMore return code).  Note that the
        // dynamic cast was already verified in activateCopyOnWrite.
        table = findInMapOrNull(tableId, m_snapshottingTables);
        if (table == nullptr) {
            return TABLE_STREAM_SERIALIZATION_ERROR;
        }

        remaining = table->streamMore(outputStreams, streamType, retPositions);
        if (remaining <= 0 && remaining > TABLE_STREAM_SERIALIZATION_ERROR_MORE_TUPLES) {
            m_snapshottingTables.erase(tableId);
            if (table->isReplicatedTable()) {
                ScopedReplicatedResourceLock scopedLock;
                ExecuteWithMpMemory usingMpMemory;
                table->decrementRefcount();
            } else {
                table->decrementRefcount();
            }
        }
    } else if (tableStreamTypeIsStreamIndexing(streamType)) {
        Table* found = getTableById(tableId);
        if (found == nullptr) {
            return TABLE_STREAM_SERIALIZATION_ERROR;
        }

        PersistentTable* currentTable = dynamic_cast<PersistentTable*>(found);
        vassert(currentTable != nullptr);
        // An ongoing TABLE STREAM INDEXING needs to continue indexing the
        // original table from before the first table truncate.
        PersistentTable* originalTable = currentTable->tableForStreamIndexing();

        VOLT_DEBUG("tableStreamSerializeMore: type %s, rewinds to the table before the first truncate",
                tableStreamTypeToString(streamType).c_str());

        remaining = originalTable->streamMore(outputStreams, streamType, retPositions);
        if (remaining <= 0) {
            // The ongoing TABLE STREAM INDEXING has finished.
            // The table no longer needs to track the original version
            // on which the INDEXING process began -- the original state will
            // likely be garbage collected now as its reference count drops to 0.
            // Or this may be deferred until a current transaction no longer needs
            // it for rollback UNDO processing.
            currentTable->unsetTableForStreamIndexing();
            VOLT_DEBUG("tableStreamSerializeMore: type %s, null the previous truncate table pointer",
                    tableStreamTypeToString(streamType).c_str());
        }
    } else {
        Table* found = getTableById(tableId);
        if (found == nullptr) {
            return TABLE_STREAM_SERIALIZATION_ERROR;
        }

        table = dynamic_cast<PersistentTable*>(found);
        remaining = table->streamMore(outputStreams, streamType, retPositions);
    }

    return remaining;
}

void VoltDBEngine::setExportStreamPositions(int64_t uso,
        int64_t seqNo, int64_t generationIdCreated, std::string streamName) {
    std::map<std::string, StreamedTable*>::iterator pos = m_exportingTables.find(streamName);

    // ignore trying to sync a non-exported table
    if (pos != m_exportingTables.end()) {
        pos->second->setExportStreamPositions(seqNo, (size_t) uso, generationIdCreated);
    }
}

bool VoltDBEngine::deleteMigratedRows(
        int64_t txnId,
        int64_t spHandle,
        int64_t uniqueId,
        std::string const& tableName,
        int64_t deletableTxnId,
        int64_t undoToken) {
    PersistentTable* table = dynamic_cast<PersistentTable*>(getTableByName(tableName));
    if (table) {
        setUndoToken(undoToken);
        m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(), txnId,
                spHandle, -1, uniqueId, false);

        /*bool hasDRBinaryLog = */ m_executorContext->checkTransactionForDR();

        ConditionalSynchronizedExecuteWithMpMemory possiblySynchronizedUseMpMemory
                (table->isReplicatedTable(), isLowestSite(), []() {
                 s_loadTableException = VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE;
                 });
        if (possiblySynchronizedUseMpMemory.okToExecute()) {
            bool rowsToBeDeleted;
            try {
                rowsToBeDeleted = table->deleteMigratedRows(deletableTxnId);
            } catch (const SQLException &sqe) {
                s_loadTableException = VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_SQL;
                throw;
            } catch (const SerializableEEException& serializableExc) {
                // Exceptions that are not constraint failures or sql exeception are treated as fatal.
                // This is legacy behavior.  Perhaps we cannot be ensured of data integrity for some mysterious
                // other kind of exception?
                s_loadTableException = serializableExc.getType();
                throwFatalException("%s", serializableExc.message().c_str());
            }

            // Indicate to other threads that load happened successfully.
            s_loadTableException = VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_NONE;
            return rowsToBeDeleted;
        } else if (s_loadTableException == VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_SQL) {
            // An sql exception was thrown on the lowest site thread and
            // handle it on the other threads too.
            std::ostringstream oss;
            oss << "Replicated table deleteMigratedRows failed (sql exception) on other thread for table \""
                << table->name() << "\".\n";
            VOLT_DEBUG("%s", oss.str().c_str());
            throw SerializableEEException(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE,
                    oss.str().c_str());
        } else if (s_loadTableException != VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_NONE) { // some other kind of exception occurred on lowest site thread
            // This is fatal.
            std::ostringstream oss;
            oss << "An unknown exception occurred on another thread calling deleteMigratedRows \"" << table->name() << "\".";
            VOLT_DEBUG("%s", oss.str().c_str());
            throwFatalException("%s", oss.str().c_str());
        }
    }
    if (LogManager::getLogLevel(LOGGERID_HOST) == LOGLEVEL_DEBUG) {
        char msg[512];
        snprintf(msg, sizeof(msg),
                "Attempted to delete migrated rows for a Table (%s) that has been removed.",
                tableName.c_str());
        msg[sizeof msg - 1] = '\0';
        LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_DEBUG, msg);
    }

    return false;
}

void VoltDBEngine::getUSOForExportTable(size_t &ackOffset, int64_t &seqNo, int64_t &genId,
        std::string const& streamName) {

    // defaults mean failure
    ackOffset = 0;
    seqNo = -1;
    genId = 0;

    auto const pos = m_exportingTables.find(streamName);
    // return no data and polled offset for unavailable tables.
    if (pos != m_exportingTables.cend()) {
        Table *table_for_el = pos->second;
        table_for_el->getExportStreamPositions(seqNo, ackOffset, genId);
    }
}

size_t VoltDBEngine::tableHashCode(int32_t tableId) {
    Table* found = getTableById(tableId);
    if (! found) {
        throwFatalException("Tried to calculate a hash code for a table that doesn't exist with id %d\n", tableId);
    }

    PersistentTable *table = dynamic_cast<PersistentTable*>(found);
    if (table == NULL) {
        throwFatalException("Tried to calculate a hash code for a table that is not a persistent table id %d\n",
                tableId);
    }
    return table->hashCode();
}

void VoltDBEngine::setHashinator(TheHashinator* hashinator) {
    m_hashinator.reset(hashinator);
}

void VoltDBEngine::updateHashinator(const char *config, int32_t *configPtr, uint32_t numTokens) {

    setHashinator(ElasticHashinator::newInstance(config, configPtr, numTokens));
}

void VoltDBEngine::dispatchValidatePartitioningTask(ReferenceSerializeInputBE &taskInfo) {
    std::vector<CatalogId> tableIds;
    const int32_t numTables = taskInfo.readInt();
    for (int ii = 0; ii < numTables; ii++) {
        tableIds.push_back(static_cast<int32_t>(taskInfo.readLong()));
    }

    const char *config = taskInfo.getRawPointer();
    TheHashinator* hashinator = ElasticHashinator::newInstance(config, NULL, 0);
    // Delete at earliest convenience
    std::unique_ptr<TheHashinator> hashinator_guard(hashinator);

    std::vector<int64_t> mispartitionedRowCounts;

    for (CatalogId tableId : tableIds) {
        std::map<CatalogId, Table*>::iterator table = m_tables.find(tableId);
        if (table == m_tables.end()) {
            throwFatalException("Unknown table id %d", tableId);
        }
        Table* found = table->second;
        mispartitionedRowCounts.push_back(found->validatePartitioning(hashinator, m_partitionId));
    }

    m_resultOutput.writeInt(static_cast<int32_t>(sizeof(int64_t) * numTables));

    for (int64_t mispartitionedRowCount : mispartitionedRowCounts) {
        m_resultOutput.writeLong(mispartitionedRowCount);
    }
}

void VoltDBEngine::collectDRTupleStreamStateInfo() {
    std::size_t size = 3 * sizeof(int64_t) + 4 /*drVersion*/ + 1 /*hasReplicatedStream*/;
    if (m_executorContext->drReplicatedStream()) {
        size += 3 * sizeof(int64_t);
    }
    m_resultOutput.writeInt(static_cast<int32_t>(size));
    DRCommittedInfo drInfo = m_executorContext->drStream()->getLastCommittedSequenceNumberAndUniqueIds();
    m_resultOutput.writeLong(drInfo.seqNum);
    m_resultOutput.writeLong(drInfo.spUniqueId);
    m_resultOutput.writeLong(drInfo.mpUniqueId);
    m_resultOutput.writeInt(m_executorContext->drStream()->drProtocolVersion());
    if (m_executorContext->drReplicatedStream()) {
        m_resultOutput.writeByte(static_cast<int8_t>(1));
        drInfo = m_executorContext->drReplicatedStream()->getLastCommittedSequenceNumberAndUniqueIds();
        m_resultOutput.writeLong(drInfo.seqNum);
        m_resultOutput.writeLong(drInfo.spUniqueId);
        m_resultOutput.writeLong(drInfo.mpUniqueId);
    } else {
        m_resultOutput.writeByte(static_cast<int8_t>(0));
    }
}

int64_t VoltDBEngine::applyBinaryLog(
        int64_t txnId,
        int64_t spHandle,
        int64_t lastCommittedSpHandle,
        int64_t uniqueId,
        int32_t remoteClusterId,
        int64_t undoToken,
        const char *logs) {
    DRTupleStreamDisableGuard guard(m_executorContext, !m_isActiveActiveDREnabled);
    setUndoToken(undoToken);
    m_executorContext->setupForPlanFragments(
            getCurrentUndoQuantum(), txnId, spHandle, lastCommittedSpHandle,
            uniqueId, false);
    // Notice: We now get the consumer drProtocolVersion from its producer side (m_drStream).
    // This assumes that all consumers use same protocol as its producer.
    // However, once we start supporting promote dr protocol (e.g. upgrade dr protocol via promote event from one coordinator cluster),
    // the coordinate cluster's consumer sides could operate in different protocols.
    // At that time, we need explicity pass in the consumer side protocol version for deciding weather
    // its corresponding remote producer has replicated stream or not.
    auto clusterEntry = m_replicableTables.find(remoteClusterId);
    if (clusterEntry == m_replicableTables.end()) {
        std::ostringstream stream("Cluster ID not found in replicable tables map: ");
        stream << remoteClusterId;
        throw SerializableEEException(stream.str());
    }
    return m_wrapper.apply(logs, clusterEntry->second, &m_stringPool, this, remoteClusterId, uniqueId);
}

void VoltDBEngine::executeTask(TaskType taskType, ReferenceSerializeInputBE &taskInfo) {
    switch (taskType) {
    case TASK_TYPE_VALIDATE_PARTITIONING:
        dispatchValidatePartitioningTask(taskInfo);
        break;
    case TASK_TYPE_GET_DR_TUPLESTREAM_STATE:
        collectDRTupleStreamStateInfo();
        break;
    case TASK_TYPE_SET_DR_SEQUENCE_NUMBERS: {
        int64_t partitionSequenceNumber = taskInfo.readLong();
        int64_t mpSequenceNumber = taskInfo.readLong();
        if (partitionSequenceNumber >= 0) {
            m_executorContext->drStream()->setLastCommittedSequenceNumber(partitionSequenceNumber);
        }
        if (m_executorContext->drReplicatedStream() && mpSequenceNumber >= 0) {
            m_executorContext->drReplicatedStream()->setLastCommittedSequenceNumber(mpSequenceNumber);
        }
        m_resultOutput.writeInt(0);
        break;
    }
    case TASK_TYPE_SET_DR_PROTOCOL_VERSION: {
        uint8_t drProtocolVersion = static_cast<uint8_t >(taskInfo.readInt());
        // create or delete dr replicated stream as needed
        if (s_drReplicatedStream == nullptr && isLowestSite()) {
            s_drReplicatedStream = new DRTupleStream(16383, m_drStream->getDefaultCapacity(), drProtocolVersion);
            m_drReplicatedStream = s_drReplicatedStream;
        }
        m_drStream->setDrProtocolVersion(drProtocolVersion);
        m_executorContext->setDrStream(m_drStream);
        m_executorContext->setDrReplicatedStream(m_drReplicatedStream);
        m_resultOutput.writeInt(0);
        break;
    }
    case TASK_TYPE_GENERATE_DR_EVENT: {
        DREventType type = (DREventType)taskInfo.readInt();
        int64_t uniqueId = taskInfo.readLong();
        int64_t lastCommittedSpHandle = taskInfo.readLong();
        int64_t spHandle = taskInfo.readLong();
        int64_t txnId = taskInfo.readLong();
        int64_t undoToken = taskInfo.readLong();
        ByteArray payloads = taskInfo.readBinaryString();

        setUndoToken(undoToken);
        m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(), txnId,
                spHandle, lastCommittedSpHandle, uniqueId, false);

        UndoQuantum *uq = ExecutorContext::currentUndoQuantum();
        vassert(uq);
        uq->registerUndoAction(
                new (*uq) ExecuteTaskUndoGenerateDREventAction(
                        m_executorContext->drStream(), m_executorContext->drReplicatedStream(),
                        m_executorContext->m_partitionId,
                        type, spHandle, uniqueId, payloads));
        break;
    }
    default:
        throwFatalException("Unknown task type %d", taskType);
    }
}

void VoltDBEngine::addToTuplesModified(int64_t amount) {
    m_executorContext->addToTuplesModified(amount);
}

void TempTableTupleDeleter::operator()(AbstractTempTable* tbl) const {
    if (tbl != NULL) {
        tbl->deleteAllTuples();
    }
}

/* (Ethan): During snapshot restore, all replicated persistent table views and explicitly partitioned
 * persistent table views will be put into paused mode, meaning that we are not going to
 * maintain the data in them while table data is being imported from the snapshot.

 * What is an implicitly partitioned view?
 * * If any of the source tables of a view is partitioned, the view is partitioned.
 * * If the partition column of any view source tables is present in the view group-by columns,
 *   the view will use the first-found such column as the partition column.
 *   In this case, the view is **explicitly partitioned**.
 * * If none of the partition column of any view source tables is included in the view group-by columns,
 *   the view is still partitioned, but the partition column is set to null.
 *   We call it **implicitly partitioned** because given any row in the view table, we have no way to
 *   infer which partition it should go to using the hashinator.

 * Why do we need to know the viewNames?
 * In the node rejoin case, all the tables in the database will be streamed to the rejoining node.
 * Knowing the name of the views we are pausing/resuming is not necessary.
 * However, because we allow partial snapshots for normal snapshots, and the database catalog may have
 * changed before a @SnapshotRestore, we need to precisely control which views to pause/resume and which
 * views to keep untouched.
 */
void VoltDBEngine::setViewsEnabled(const std::string& viewNames, bool value) {
    // We need to update the statuses of replicated views and partitioned views separately to consolidate
    // the use of the count-down latch which is required when updating replicated views.
    VOLT_TRACE("[Partition %d] VoltDBEngine::setViewsEnabled(%s, %s)\n", m_partitionId, viewNames.c_str(), value?"true":"false");
    bool updateReplicated = false;
    // The loop below is executed exactly twice. The first iteration has updateReplicated = false, where we
    // update the partitioned views. The updateReplicated flag is flipped to true at the end of the iteration,
    // which allows the loop to execute for a second round to update replicated views.
    do {
        VOLT_TRACE("[Partition %d] updateReplicated = %s\n", m_partitionId, updateReplicated?"true":"false");
        // Update all the partitioned table views first, then update all the replicated table views.
        ConditionalSynchronizedExecuteWithMpMemory possiblySynchronizedUseMpMemory(
                updateReplicated, isLowestSite(), [](){});
        if (possiblySynchronizedUseMpMemory.okToExecute()) {
            // This loop just split the viewNames by commas and process each view individually.
            for (size_t pstart = 0, pend = 0; pstart != std::string::npos; pstart = pend) {
                std::string viewName = viewNames.substr(pstart+(pstart!=0), (pend=viewNames.find(',',pstart+1))-pstart-(pstart!=0));
                Table *table = getTableByName(viewName);
                PersistentTable *persistentTable = dynamic_cast<PersistentTable*>(table);
                if (! persistentTable) {
                    // We do not look at export tables.
                    // We should have prevented this in the Java layer.
                    continue;
                } else if (persistentTable->isReplicatedTable() != updateReplicated) {
                    VOLT_TRACE("[Partition %d] skip %s\n", m_partitionId, persistentTable->name().c_str());
                    continue;
                } else if (persistentTable->materializedViewTrigger()) {
                    VOLT_TRACE("[Partition %d] %s->materializedViewTrigger()->setEnabled(%s)\n",
                               m_partitionId, persistentTable->name().c_str(), value?"true":"false");
                    // Single table view
                    persistentTable->materializedViewTrigger()->setEnabled(value);
                } else if (persistentTable->materializedViewHandler()) {
                    VOLT_TRACE("[Partition %d] %s->materializedViewHandler()->setEnabled(%s)\n",
                               m_partitionId, persistentTable->name().c_str(), value?"true":"false");
                    // Joined view.
                    persistentTable->materializedViewHandler()->setEnabled(value);
                }
            }
        }
        updateReplicated = ! updateReplicated;
    } while (updateReplicated);
}

void VoltDBEngine::disableExternalStreams() {
    m_executorContext->disableExternalStreams();
}

bool VoltDBEngine::externalStreamsEnabled() {
    return m_executorContext->externalStreamsEnabled();
}

int32_t VoltDBEngine::storeTopicsGroup(int64_t undoToken, SerializeInputBE& in){
    setUndoToken(undoToken);
    try {
        m_groupStore->storeGroup(in);
        return 0;
    } catch (const SerializableEEException& e) {
        serializeException(e);
    }
    return 1;
}

int32_t VoltDBEngine::deleteTopicsGroup(int64_t undoToken, const NValue& groupId){
    setUndoToken(undoToken);
    try {
        m_groupStore->deleteGroup(groupId);
        return 0;
    } catch (const SerializableEEException& e) {
        serializeException(e);
    }
    return 1;
}

int32_t VoltDBEngine::fetchTopicsGroups(int32_t maxResultSize, const NValue& startGroupId) {
    resetReusedResultOutputBuffer();
    try {
        return m_groupStore->fetchGroups(maxResultSize, startGroupId, m_resultOutput) ? 1 : 0;
    } catch (const SerializableEEException& e) {
        serializeException(e);
    }
    return -1;
}

int32_t VoltDBEngine::commitTopicsGroupOffsets(int64_t spUniqueId, int64_t undoToken, int16_t requestVersion,
        const NValue& groupId, SerializeInputBE& in) {
    setUndoToken(undoToken);
    resetReusedResultOutputBuffer();
    try {
        m_groupStore->commitOffsets(UniqueId::ts(spUniqueId), requestVersion, groupId, in, m_resultOutput);
        return 0;
    } catch (const SerializableEEException& e) {
        serializeException(e);
    }
    return 1;
}

int32_t VoltDBEngine::fetchTopicsGroupOffsets(int16_t requestVersion, const NValue& groupId, SerializeInputBE& in) {
    resetReusedResultOutputBuffer();
    try {
        m_groupStore->fetchOffsets(requestVersion, groupId, in, m_resultOutput);
        return 0;
    } catch (const SerializableEEException& e) {
        serializeException(e);
    }
    return 1;
}

int32_t VoltDBEngine::deleteExpiredTopicsOffsets(int64_t undoToken, int64_t deleteOlderThan) {
    setUndoToken(undoToken);
    try {
        m_groupStore->deleteExpiredOffsets(deleteOlderThan);
        return 0;
    } catch (const SerializableEEException& e) {
        serializeException(e);
    }
    return 1;
}

int32_t VoltDBEngine::setReplicableTables(int32_t clusterId, const std::vector<std::string>* replicableTables) {
    try {
        if (replicableTables == nullptr) {
            m_replicableTables.erase(clusterId);
            return 0;
        }

        auto& tablesByHash = m_replicableTables[clusterId];
        tablesByHash.clear();

        for (const std::string& tableName : *replicableTables) {
            TableCatalogDelegate* delegate = getTableDelegate(tableName);
            if (delegate == nullptr) {
                continue;
            }

            PersistentTable* table = delegate->getPersistentTable();
            if (table == nullptr) {
                vassert(false);
                continue;
            }

            int64_t hash = delegate->signatureHashAsLong();
            tablesByHash[hash] = table;
        }

        return 0;
    } catch (const SerializableEEException& e) {
        serializeException(e);
    }
    return 1;
}

void VoltDBEngine::clearReplicableTables(int clusterId) {
    if (clusterId < 0) {
        return;
    }
    auto clusterEntry = m_replicableTables.find(clusterId);
    if (clusterEntry == m_replicableTables.end()) {
        return;
    }
    clusterEntry->second.clear();
    m_replicableTables.erase(clusterEntry);
}

void VoltDBEngine::clearAllReplicableTables() {
    m_replicableTables.clear();
}

void VoltDBEngine::loadBuiltInJavaFunctions() {
    // Hard code the info of format_timestamp function
    UserDefinedFunctionInfo *info = new UserDefinedFunctionInfo();
    info->returnType = ValueType::tVARCHAR;
    info->paramTypes.resize(2);
    info->paramTypes.at(0) = ValueType::tTIMESTAMP;
    info->paramTypes.at(1) = ValueType::tVARCHAR;

    m_functionInfo[FUNC_VOLT_FORMAT_TIMESTAMP] = info;
}

} // namespace voltdb
