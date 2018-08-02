/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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
#include "catalog/connector.h"
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
#include "common/RecoveryProtoMessage.h"
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
#include "storage/TableCatalogDelegate.hpp"
#include "storage/tablefactory.h"
#include "storage/temptable.h"
#include "storage/ConstraintFailureException.h"
#include "storage/DRTupleStream.h"

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
typedef boost::multi_index::multi_index_container<
    boost::shared_ptr<ExecutorVector>,
    boost::multi_index::indexed_by<
        boost::multi_index::sequenced<>,
        boost::multi_index::hashed_unique<
            boost::multi_index::const_mem_fun<ExecutorVector,int64_t,&ExecutorVector::getFragId>
        >
    >
> PlanSet;

  int32_t s_exportFlushTimeout=4000;  // export/tuple flush interval ms setting


/// This class wrapper around a typedef allows forward declaration as in scoped_ptr<EnginePlanSet>.
class EnginePlanSet : public PlanSet { };

VoltEEExceptionType VoltDBEngine::s_loadTableException = VOLT_EE_EXCEPTION_TYPE_NONE;

VoltDBEngine::VoltDBEngine(Topend* topend, LogProxy* logProxy)
    : m_currentIndexInBatch(-1),
      m_currentUndoQuantum(NULL),
      m_siteId(-1),
      m_isLowestSite(false),
      m_partitionId(-1),
      m_hashinator(NULL),
      m_isActiveActiveDREnabled(false),
      m_currentInputDepId(-1),
      m_stringPool(16777216, 2),
      m_numResultDependencies(0),
      m_logManager(logProxy),
      m_templateSingleLongTable(NULL),
      m_topend(topend),
      m_executorContext(NULL),
      m_drPartitionedConflictStreamedTable(NULL),
      m_drReplicatedConflictStreamedTable(NULL),
      m_drStream(NULL),
      m_drReplicatedStream(NULL),
      m_currExecutorVec(NULL)
{
    loadBuiltInJavaFunctions();
}

void
VoltDBEngine::initialize(int32_t clusterIndex,
                         int64_t siteId,
                         int32_t partitionId,
                         int32_t sitesPerHost,
                         int32_t hostId,
                         std::string hostname,
                         int32_t drClusterId,
                         int32_t defaultDrBufferSize,
                         int64_t tempTableMemoryLimit,
                         bool isLowestSiteId,
                         int32_t compactionThreshold,
                         int32_t exportFlushTimeout)
{
    m_clusterIndex = clusterIndex;
    m_siteId = siteId;
    m_isLowestSite = isLowestSiteId;
    m_partitionId = partitionId;
    m_tempTableMemoryLimit = tempTableMemoryLimit;
    m_compactionThreshold = compactionThreshold;
    s_exportFlushTimeout = exportFlushTimeout;

    // Instantiate our catalog - it will be populated later on by load()
    m_catalog.reset(new catalog::Catalog());

    // create the template single long (int) table
    assert (m_templateSingleLongTable == NULL);
    m_templateSingleLongTable = new char[m_templateSingleLongTableSize];
    memset(m_templateSingleLongTable, 0, m_templateSingleLongTableSize);
    m_templateSingleLongTable[7] = 43;  // size through start of data?
    m_templateSingleLongTable[11] = 23; // size of header
    m_templateSingleLongTable[13] = 0;  // status code
    m_templateSingleLongTable[14] = 1;  // number of columns
    m_templateSingleLongTable[15] = VALUE_TYPE_BIGINT; // column type
    m_templateSingleLongTable[19] = 15; // column name length:  "modified_tuples" == 15
    m_templateSingleLongTable[20] = 'm';
    m_templateSingleLongTable[21] = 'o';
    m_templateSingleLongTable[22] = 'd';
    m_templateSingleLongTable[23] = 'i';
    m_templateSingleLongTable[24] = 'f';
    m_templateSingleLongTable[25] = 'i';
    m_templateSingleLongTable[26] = 'e';
    m_templateSingleLongTable[27] = 'd';
    m_templateSingleLongTable[28] = '_';
    m_templateSingleLongTable[29] = 't';
    m_templateSingleLongTable[30] = 'u';
    m_templateSingleLongTable[31] = 'p';
    m_templateSingleLongTable[32] = 'l';
    m_templateSingleLongTable[33] = 'e';
    m_templateSingleLongTable[34] = 's';
    m_templateSingleLongTable[38] = 1; // row count
    m_templateSingleLongTable[42] = 8; // row size

    // configure DR stream
    m_drStream = new DRTupleStream(partitionId, static_cast<size_t>(defaultDrBufferSize));

    // required for catalog loading.
    m_executorContext = new ExecutorContext(siteId,
                                            m_partitionId,
                                            m_currentUndoQuantum,
                                            getTopend(),
                                            &m_stringPool,
                                            this,
                                            hostname,
                                            hostId,
                                            m_drStream,
                                            m_drReplicatedStream,
                                            drClusterId);
    // Add the engine to the global list tracking replicated tables
    SynchronizedThreadLock::lockReplicatedResourceForInit();
    ThreadLocalPool::setPartitionIds(m_partitionId);
    VOLT_DEBUG("Initializing partition %d (tid %ld) with context %p", m_partitionId,
            SynchronizedThreadLock::getThreadId(), m_executorContext);
    EngineLocals newLocals = EngineLocals(ExecutorContext::getExecutorContext());
    SynchronizedThreadLock::init(sitesPerHost, newLocals);
    SynchronizedThreadLock::unlockReplicatedResourceForInit();
}

VoltDBEngine::~VoltDBEngine() {
    // WARNING WARNING WARNING
    // The sequence below in which objects are cleaned up/deleted is
    // fragile.  Reordering or adding additional destruction below
    // greatly increases the risk of accidentally freeing the same
    // object multiple times.  Change at your own risk.
    // --izzy 8/19/2009

    // clean up execution plans
    m_plans.reset();

    // Clear the undo log before deleting the persistent tables so
    // that the persistent table schema are still around so we can
    // actually find the memory that has been allocated to non-inlined
    // strings and deallocated it.
    m_undoLog.clear();

    // clean up memory for the template memory for the single long (int) table
    if (m_templateSingleLongTable) {
        delete[] m_templateSingleLongTable;
    }

    // Delete table delegates and release any table reference counts.
    typedef std::pair<int64_t, Table*> TID;

    if (m_partitionId != 16383) {
        for (auto tcdIter = m_catalogDelegates.cbegin(); tcdIter != m_catalogDelegates.cend(); ) {
            auto eraseThis = tcdIter;
            tcdIter++;
            auto table = eraseThis->second->getPersistentTable();
            bool deleteWithMpPool = false;
            if (!table) {
                VOLT_DEBUG("Partition %d Deallocating %s table", m_partitionId, eraseThis->second->getTable()->name().c_str());
            }
            else if(!table->isCatalogTableReplicated()) {
                VOLT_DEBUG("Partition %d Deallocating partitioned table %s", m_partitionId, eraseThis->second->getTable()->name().c_str());
            }
            else {
                deleteWithMpPool = true;
                VOLT_DEBUG("Partition %d Deallocating replicated table %s", m_partitionId, eraseThis->second->getTable()->name().c_str());
            }

            if (deleteWithMpPool) {
                if (m_isLowestSite) {
                    ScopedReplicatedResourceLock scopedLock;
                    ExecuteWithMpMemory usingMpMemory;
                    delete eraseThis->second;
                }
            }
            else {
                delete eraseThis->second;
            }

            m_catalogDelegates.erase(eraseThis->first);
        }

        if (m_isLowestSite) {
            SynchronizedThreadLock::resetMemory(SynchronizedThreadLock::s_mpMemoryPartitionId);
        }
        BOOST_FOREACH (TID tid, m_snapshottingTables) {
            tid.second->decrementRefcount();
        }

        BOOST_FOREACH (auto labeledInfo, m_functionInfo) {
            delete labeledInfo.second;
        }

        delete m_executorContext;

        delete m_drReplicatedStream;
        delete m_drStream;
    }
    else {
        delete m_executorContext;
    }
    VOLT_DEBUG("finished deallocate for partition %d", m_partitionId);
}

// ------------------------------------------------------------------
// OBJECT ACCESS FUNCTIONS
// ------------------------------------------------------------------
catalog::Catalog* VoltDBEngine::getCatalog() const {
    return (m_catalog.get());
}

Table* VoltDBEngine::getTableById(int32_t tableId) const {
    // Caller responsible for checking null return value.
    return findInMapOrNull(tableId, m_tables);
}

Table* VoltDBEngine::getTableByName(const std::string& name) const {
    // Caller responsible for checking null return value.
    return findInMapOrNull(name, m_tablesByName);
}

TableCatalogDelegate* VoltDBEngine::getTableDelegate(const std::string& name) const {
    // Caller responsible for checking null return value.
    return findInMapOrNull(name, m_delegatesByName);
}

catalog::Table* VoltDBEngine::getCatalogTable(const std::string& name) const {
    // iterate over all of the tables in the new catalog
    BOOST_FOREACH (LabeledTable labeledTable, m_database->tables()) {
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
    if ( ! table) {
        throwFatalException("Unable to find table for TableId '%d'", (int) tableId);
    }
    table->serializeTo(out);
}

// ------------------------------------------------------------------
// EXECUTION FUNCTIONS
// ------------------------------------------------------------------
/**
 * Execute the given fragments serially and in order.
 * Return 0 if there are no failures and 1 if there are
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
int VoltDBEngine::executePlanFragments(int32_t numFragments,
                                       int64_t planfragmentIds[],
                                       int64_t inputDependencyIds[],
                                       ReferenceSerializeInputBE &serialInput,
                                       int64_t txnId,
                                       int64_t spHandle,
                                       int64_t lastCommittedSpHandle,
                                       int64_t uniqueId,
                                       int64_t undoToken,
                                       bool traceOn)
{
    // count failures
    int failures = 0;

    setUndoToken(undoToken);

    // configure the execution context.
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(),
                                             txnId,
                                             spHandle,
                                             lastCommittedSpHandle,
                                             uniqueId,
                                             traceOn);

    bool hasDRBinaryLog = m_executorContext->checkTransactionForDR();

    // reset these at the start of each batch
    m_executorContext->m_progressStats.resetForNewBatch();
    NValueArray &params = m_executorContext->getParameterContainer();

    // Reserve the space to track the number of succeeded fragments.
    size_t succeededFragmentsCountOffset = m_perFragmentStatsOutput.reserveBytes(sizeof(int32_t));
    // All the time measurements use nanoseconds.
    std::chrono::high_resolution_clock::time_point startTime, endTime;
    std::chrono::duration<int64_t, std::nano> elapsedNanoseconds;
    ReferenceSerializeInputBE perFragmentStatsBufferIn(getPerFragmentStatsBuffer(),
                                                       getPerFragmentStatsBufferCapacity());
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
        assert (usedParamcnt <= MAX_PARAM_COUNT);

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
                drBufferChange = m_drStream->m_uso - m_drStream->m_committedUso;
                assert(drBufferChange >= DRTupleStream::BEGIN_RECORD_SIZE);
                drBufferChange -= DRTupleStream::BEGIN_RECORD_SIZE;
            }
            if (m_drReplicatedStream) {
                size_t drReplicatedStreamBufferChange = m_drReplicatedStream->m_uso - m_drReplicatedStream->m_committedUso;
                assert(drReplicatedStreamBufferChange >= DRTupleStream::BEGIN_RECORD_SIZE);
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

int VoltDBEngine::executePlanFragment(int64_t planfragmentId,
                                      int64_t inputDependencyId,
                                      bool traceOn)
{
    assert(planfragmentId != 0);

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
    assert(m_executorContext->getModifiedTupleStackSize() == 0);

    int64_t tuplesModified = 0;
    try {
        // execution lists for planfragments are cached by planfragment id
        setExecutorVectorForFragmentId(planfragmentId);
        assert(m_currExecutorVec);

        executePlanFragment(m_currExecutorVec, &tuplesModified);
    }
    catch (const SerializableEEException &e) {
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

UniqueTempTableResult VoltDBEngine::executePlanFragment(ExecutorVector* executorVector, int64_t* tuplesModified) {
    UniqueTempTableResult result;
    // set this to zero for dml operations
    m_executorContext->pushNewModifiedTupleCounter();

    // execution lists for planfragments are cached by planfragment id
    try {
        // Launch the target plan through its top-most executor list.
        executorVector->setupContext(m_executorContext);
        result = m_executorContext->executeExecutors(0);
    }
    catch (const SerializableEEException &e) {
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
    for (int i = 0; i < arguments.size(); i++) {
        // It is very common that the argument we are going to pass is in
        // a compatible data type which does not exactly match the type that
        // is defined in the function.
        // We need to cast it to the target data type before the serialization.
        arguments[i] = arguments[i].castAs(info->paramTypes[i]);
        bufferSizeNeeded += arguments[i].serializedSize();
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
    for (int i = 0; i < arguments.size(); i++) {
        arguments[i].serializeTo(m_udfOutput);
    }
    // Make sure we did the correct size calculation.
    assert(bufferSizeNeeded + sizeof(int32_t) == m_udfOutput.position());

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
    }
    else {
        // Error handling
        string errorMsg = udfResultIn.readTextString();
        throw SQLException(SQLException::volt_user_defined_function_error, errorMsg);
    }
}

void VoltDBEngine::releaseUndoToken(int64_t undoToken, bool isEmptyDRTxn) {
    if (m_currentUndoQuantum != NULL && m_currentUndoQuantum->getUndoToken() == undoToken) {
        m_currentUndoQuantum = NULL;
        m_executorContext->setupForPlanFragments(NULL);
    }
    m_undoLog.release(undoToken);

    if (isEmptyDRTxn) {
        if (m_executorContext->drStream()) {
            m_executorContext->drStream()->rollbackTo(m_executorContext->drStream()->m_committedUso, SIZE_MAX);
        }
        if (m_executorContext->drReplicatedStream()) {
            m_executorContext->drReplicatedStream()->rollbackTo(m_executorContext->drReplicatedStream()->m_committedUso, SIZE_MAX);
        }
    }
    else {
        if (m_executorContext->drStream()) {
            m_executorContext->drStream()->endTransaction(m_executorContext->currentUniqueId());
        }
        if (m_executorContext->drReplicatedStream()) {
            m_executorContext->drReplicatedStream()->endTransaction(m_executorContext->currentUniqueId());
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

    return true;
}

bool VoltDBEngine::loadCatalog(const int64_t timestamp, const std::string &catalogPayload) {
    assert(m_executorContext != NULL);
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
    assert(m_catalog != NULL);
    VOLT_DEBUG("Loading catalog on partition %d ...", m_partitionId);


    m_catalog->execute(catalogPayload);


    if (updateCatalogDatabaseReference() == false) {
        return false;
    }

    // Set DR flag based on current catalog state
    catalog::Cluster* catalogCluster = m_catalog->clusters().get("cluster");
    m_executorContext->drStream()->m_enabled = catalogCluster->drProducerEnabled();
    m_executorContext->drStream()->m_flushInterval = catalogCluster->drFlushInterval();
    if (m_executorContext->drReplicatedStream()) {
        m_executorContext->drReplicatedStream()->m_enabled = m_executorContext->drStream()->m_enabled;
        m_executorContext->drReplicatedStream()->m_flushInterval = m_executorContext->drStream()->m_flushInterval;
    }

    VOLT_DEBUG("loading partitioned parts of catalog from partition %d", m_partitionId);

    //When loading catalog we do isStreamUpdate to true as we are starting fresh or rejoining/recovering.
    std::map<std::string, ExportTupleStream*> purgedStreams;
    if (processCatalogAdditions(timestamp, false, true, purgedStreams) == false) {
        return false;
    }

    rebuildTableCollections();

    if (SynchronizedThreadLock::countDownGlobalTxnStartCount(m_isLowestSite)) {
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
    if (SynchronizedThreadLock::countDownGlobalTxnStartCount(m_isLowestSite)) {
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
void
VoltDBEngine::processCatalogDeletes(int64_t timestamp, bool updateReplicated,
        std::map<std::string, ExportTupleStream*> & purgedStreams)
{
    std::vector<std::string> deletion_vector;
    m_catalog->getDeletedPaths(deletion_vector);
    std::set<std::string> deletions(deletion_vector.begin(), deletion_vector.end());
    // Filter out replicated or partitioned deletions
    std::set<std::string>::iterator it = deletions.begin();
    while (it != deletions.end()) {
        std::string path = *it++;
        auto pos = m_catalogDelegates.find(path);
        if (pos == m_catalogDelegates.end()) {
           continue;
        }
        auto tcd = pos->second;
        assert(tcd);
        if (tcd) {
            Table* table = tcd->getTable();
            PersistentTable * persistenttable = dynamic_cast<PersistentTable*>(table);
            if (persistenttable) {
                if (updateReplicated != persistenttable->isCatalogTableReplicated()) {
                    deletions.erase(path);
                }
            }
        }
    }

    // delete any empty persistent tables, forcing them to be rebuilt
    // (Unless the are actually being deleted -- then this does nothing)

    BOOST_FOREACH (LabeledTCD delegatePair, m_catalogDelegates) {
        auto tcd = delegatePair.second;
        Table* table = tcd->getTable();

        // skip export tables for now
        if (dynamic_cast<StreamedTable*>(table)) {
            continue;
        }
        if (table->activeTupleCount() == 0) {
            PersistentTable *persistenttable = dynamic_cast<PersistentTable*>(table);
            if (persistenttable) {
                if (persistenttable->isCatalogTableReplicated()) {
                    if (updateReplicated) {
                        // identify empty tables and mark for deletion
                        deletions.insert(delegatePair.first);
                    }
                } else {
                    if (!updateReplicated) {
                        // identify empty tables and mark for deletion
                        deletions.insert(delegatePair.first);
                    }
                }
            }
        }
    }

    auto catalogFunctions = m_database->functions();

    // delete tables in the set
    bool isReplicatedTable;
    BOOST_FOREACH (auto path, deletions) {
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
        assert(tcd);
        if (tcd) {
            Table* table = tcd->getTable();
            PersistentTable * persistenttable = dynamic_cast<PersistentTable*>(table);
            if (persistenttable && persistenttable->isCatalogTableReplicated()) {
                isReplicatedTable = true;
                ExecuteWithAllSitesMemory execAllSites;
                for (auto engineIt = execAllSites.begin(); engineIt != execAllSites.end(); ++engineIt) {
                    EngineLocals& curr = engineIt->second;
                    VoltDBEngine* currEngine = curr.context->getContextEngine();
                    SynchronizedThreadLock::assumeSpecificSiteContext(curr);
                    currEngine->m_delegatesByName.erase(table->name());
                }
            } else {
                m_delegatesByName.erase(table->name());
            }
            auto streamedtable = dynamic_cast<StreamedTable*>(table);
            if (streamedtable) {
                const std::string signature = tcd->signature();
                streamedtable->setSignatureAndGeneration(signature, timestamp);
                //Maintain the streams that will go away for which wrapper needs to be cleaned;
                purgedStreams[signature] = streamedtable->getWrapper();
                //Unset wrapper so it can be deleted after last push.
                streamedtable->setWrapper(NULL);
                m_exportingTables.erase(signature);
            }
            if (isReplicatedTable) {
                VOLT_TRACE("delete a REPLICATED table %s", tcd->getTable()->name().c_str());
                ExecuteWithMpMemory usingMpMemory;
                delete tcd;
            }
            else {
                VOLT_TRACE("delete a PARTITIONED table %s", tcd->getTable()->name().c_str());
                delete tcd;
            }
        }
        if (isReplicatedTable) {
            ExecuteWithAllSitesMemory execAllSites;
            for (auto engineIt = execAllSites.begin(); engineIt != execAllSites.end(); ++engineIt) {
                EngineLocals& curr = engineIt->second;
                VoltDBEngine* currEngine = curr.context->getContextEngine();
                SynchronizedThreadLock::assumeSpecificSiteContext(curr);
                currEngine->m_catalogDelegates.erase(path);
            }
        } else {
            m_catalogDelegates.erase(path);
        }
    }
}

static bool haveDifferentSchema(catalog::Table* t1, voltdb::PersistentTable* t2) {
    // covers column count
    if (t1->columns().size() != t2->columnCount()) {
        return true;
    }

    if (t1->isDRed() != t2->isDREnabled()) {
        return true;
    }

    // make sure each column has same metadata
    std::map<std::string, catalog::Column*>::const_iterator outerIter;
    for (outerIter = t1->columns().begin();
         outerIter != t1->columns().end();
         outerIter++) {
        int index = outerIter->second->index();
        int size = outerIter->second->size();
        int32_t type = outerIter->second->type();
        std::string name = outerIter->second->name();
        bool nullable = outerIter->second->nullable();
        bool inBytes = outerIter->second->inbytes();

        if (t2->columnName(index).compare(name)) {
            return true;
        }

        auto columnInfo = t2->schema()->getColumnInfo(index);

        if (columnInfo->allowNull != nullable) {
            return true;
        }

        if (columnInfo->getVoltType() != type) {
            return true;
        }

        // check the size of types where size matters
        if ((type == VALUE_TYPE_VARCHAR) || (type == VALUE_TYPE_VARBINARY)) {
            if (columnInfo->length != size) {
                return true;
            }
            if (columnInfo->inBytes != inBytes) {
                assert(type == VALUE_TYPE_VARCHAR);
                return true;
            }
        }
    }

    return false;
}

/*
 * Create catalog delegates for new catalog tables.
 * Create the tables themselves when new tables are needed.
 * Add and remove indexes if indexes are added or removed from an
 * existing table.
 * Use the txnId of the catalog update as the generation for export
 * data.
 */
bool
VoltDBEngine::processCatalogAdditions(int64_t timestamp, bool updateReplicated,
        bool isStreamUpdate, std::map<std::string, ExportTupleStream*> & purgedStreams)
{
    // iterate over all of the tables in the new catalog
    BOOST_FOREACH (LabeledTable labeledTable, m_database->tables()) {
        // get the catalog's table object
        auto catalogTable = labeledTable.second;
        // get the delegate for the table... add the table if it's null
        auto tcd = findInMapOrNull(catalogTable->path(), m_catalogDelegates);
        if (!tcd) {
            //////////////////////////////////////////
            // add a completely new table
            //////////////////////////////////////////

            if (catalogTable->isreplicated()) {
                if (updateReplicated) {
                    assert(SynchronizedThreadLock::isLowestSiteContext());
                    ExecuteWithMpMemory useMpMemory;
                    tcd = new TableCatalogDelegate(catalogTable->signature(),
                                                   m_compactionThreshold, this);
                    // use the delegate to init the table and create indexes n' stuff
                    tcd->init(*m_database, *catalogTable, m_isActiveActiveDREnabled);
                    const std::string& tableName = tcd->getTable()->name();
                    VOLT_TRACE("add a REPLICATED completely new table or rebuild an empty table %s", tableName.c_str());
                    {
                        ExecuteWithAllSitesMemory execAllSites;
                        for (auto engineIt = execAllSites.begin(); engineIt != execAllSites.end(); ++engineIt) {
                            EngineLocals &curr = engineIt->second;
                            VoltDBEngine *currEngine = curr.context->getContextEngine();
                            currEngine->m_catalogDelegates[catalogTable->path()] = tcd;
                            currEngine->m_delegatesByName[tableName] = tcd;
                        }
                    }
                    assert(tcd->getStreamedTable() == NULL);
                }
                continue;
            }
            else {
                if (updateReplicated) {
                    continue;
                }
                tcd = new TableCatalogDelegate(catalogTable->signature(),
                                               m_compactionThreshold, this);
                // use the delegate to init the table and create indexes n' stuff
                tcd->init(*m_database, *catalogTable, m_isActiveActiveDREnabled);
                m_catalogDelegates[catalogTable->path()] = tcd;
                Table* table = tcd->getTable();
                VOLT_TRACE("add a PARTITIONED completely new table or rebuild an empty table %s", table->name().c_str());
                m_delegatesByName[table->name()] = tcd;
            }
            // set export info on the new table
            auto streamedtable = tcd->getStreamedTable();
            if (streamedtable) {
                streamedtable->setSignatureAndGeneration(catalogTable->signature(), timestamp);
                m_exportingTables[catalogTable->signature()] = streamedtable;
                if (tcd->exportEnabled()) {
                    ExportTupleStream *wrapper = m_exportingStreams[catalogTable->signature()];
                    if (wrapper == NULL) {
                        wrapper = new ExportTupleStream(m_executorContext->m_partitionId,
                                m_executorContext->m_siteId, timestamp, catalogTable->signature());
                        m_exportingStreams[catalogTable->signature()] = wrapper;
                    } else {
                        // If stream was dropped in UAC and the added back we should not purge the wrapper.
                        // A case when exact same stream is dropped and added.
                        purgedStreams[catalogTable->signature()] = NULL;
                    }
                    streamedtable->setWrapper(wrapper);
                }

                std::vector<catalog::MaterializedViewInfo*> survivingInfos;
                std::vector<MaterializedViewTriggerForStreamInsert*> survivingViews;
                std::vector<MaterializedViewTriggerForStreamInsert*> obsoleteViews;

                const catalog::CatalogMap<catalog::MaterializedViewInfo>& views = catalogTable->views();

                MaterializedViewTriggerForStreamInsert::segregateMaterializedViews(streamedtable->views(),
                        views.begin(), views.end(),
                        survivingInfos, survivingViews, obsoleteViews);

                for (int ii = 0; ii < survivingInfos.size(); ++ii) {
                    auto currInfo = survivingInfos[ii];
                    auto currView = survivingViews[ii];
                    PersistentTable* oldDestTable = currView->destTable();
                    // Use the now-current definiton of the target table, to be updated later, if needed.
                    auto targetDelegate = findInMapOrNull(oldDestTable->name(),
                                                          m_delegatesByName);
                    PersistentTable* destTable = oldDestTable; // fallback value if not (yet) redefined.
                    if (targetDelegate) {
                        auto newDestTable = targetDelegate->getPersistentTable();
                        if (newDestTable) {
                            destTable = newDestTable;
                        }
                    }
                    // This guards its destTable from accidental deletion with a refcount bump.
                    MaterializedViewTriggerForStreamInsert::build(streamedtable, destTable, currInfo);
                    obsoleteViews.push_back(currView);
                }

                BOOST_FOREACH (auto toDrop, obsoleteViews) {
                    streamedtable->dropMaterializedView(toDrop);
                }

            }
        }
        else {
            if (catalogTable->isreplicated() != updateReplicated) {
                // replicated tables should only be processed once for the entire cluster
                continue;
            }

            //////////////////////////////////////////////
            // update the export info for existing tables can not be done now so ignore streamed table.
            //
            // add/modify/remove indexes that have changed
            //  in the catalog
            //////////////////////////////////////////////
            /*
             * Instruct the table that was not added but is being retained to flush
             * Then tell it about the new export generation/catalog txnid
             * which will cause it to notify the topend export data source
             * that no more data is coming for the previous generation
             */
            auto streamedTable = tcd->getStreamedTable();
            if (streamedTable) {
                //Dont update and roll generation if this is just a non stream table update.
                if (isStreamUpdate) {
                    streamedTable->setSignatureAndGeneration(catalogTable->signature(), timestamp);
                    if (!tcd->exportEnabled()) {
                        // Evaluate export enabled or not and cache it on the tcd.
                        tcd->evaluateExport(*m_database, *catalogTable);
                        // If enabled hook up streamer
                        if (tcd->exportEnabled()) {
                            //Reset generation after stream wrapper is created.
                            streamedTable->setSignatureAndGeneration(catalogTable->signature(), timestamp);
                            m_exportingTables[catalogTable->signature()] = streamedTable;
                            ExportTupleStream *wrapper = m_exportingStreams[catalogTable->signature()];
                            if (wrapper == NULL) {
                                wrapper = new ExportTupleStream(m_executorContext->m_partitionId,
                                        m_executorContext->m_siteId, timestamp, catalogTable->signature());
                                m_exportingStreams[catalogTable->signature()] = wrapper;
                            } else {
                                //If stream was altered in UAC and the added back we should not purge the wrapper.
                                //A case when alter has not changed anything that changes table signature.
                                purgedStreams[catalogTable->signature()] = NULL;
                            }
                            streamedTable->setWrapper(wrapper);
                        }
                    }
                }

                // Deal with views
                std::vector<catalog::MaterializedViewInfo*> survivingInfos;
                std::vector<MaterializedViewTriggerForStreamInsert*> survivingViews;
                std::vector<MaterializedViewTriggerForStreamInsert*> obsoleteViews;

                const catalog::CatalogMap<catalog::MaterializedViewInfo> & views = catalogTable->views();

                MaterializedViewTriggerForStreamInsert::segregateMaterializedViews(streamedTable->views(),
                        views.begin(), views.end(),
                        survivingInfos, survivingViews, obsoleteViews);

                for (int ii = 0; ii < survivingInfos.size(); ++ii) {
                    auto currInfo = survivingInfos[ii];
                    auto currView = survivingViews[ii];
                    PersistentTable* oldDestTable = currView->destTable();
                    // Use the now-current definiton of the target table, to be updated later, if needed.
                    auto targetDelegate = findInMapOrNull(oldDestTable->name(),
                                                          m_delegatesByName);
                    PersistentTable* destTable = oldDestTable; // fallback value if not (yet) redefined.
                    if (targetDelegate) {
                        auto newDestTable = targetDelegate->getPersistentTable();
                        if (newDestTable) {
                            destTable = newDestTable;
                        }
                    }
                    // This is not a leak -- the view metadata is self-installing into the new table.
                    // Also, it guards its destTable from accidental deletion with a refcount bump.
                    MaterializedViewTriggerForStreamInsert::build(streamedTable, destTable, currInfo);
                    obsoleteViews.push_back(currView);
                }

                BOOST_FOREACH (auto toDrop, obsoleteViews) {

                    streamedTable->dropMaterializedView(toDrop);
                }
                // note, this is the end of the line for export tables for now,
                // don't allow them to change schema yet
                continue;
            }

            PersistentTable *persistentTable = tcd->getPersistentTable();

            //////////////////////////////////////////
            // if the table schema has changed, build a new
            // table and migrate tuples over to it, repopulating
            // indexes as we go
            //////////////////////////////////////////

            if (haveDifferentSchema(catalogTable, persistentTable)) {
                char msg[512];
                snprintf(msg, sizeof(msg), "Table %s has changed schema and will be rebuilt.",
                         catalogTable->name().c_str());
                LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_DEBUG, msg);
                ConditionalExecuteWithMpMemory useMpMemoryIfReplicated(updateReplicated);
                tcd->processSchemaChanges(*m_database, *catalogTable, m_delegatesByName, m_isActiveActiveDREnabled);

                snprintf(msg, sizeof(msg), "Table %s was successfully rebuilt with new schema.",
                         catalogTable->name().c_str());
                LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_DEBUG, msg);

                // don't continue on to modify/add/remove indexes, because the
                // call above should rebuild them all anyway
                continue;
            }

            //
            // Same schema, but TUPLE_LIMIT may change.
            // Because there is no table rebuilt work next, no special need to take care of
            // the new tuple limit.
            //
            persistentTable->setTupleLimit(catalogTable->tuplelimit());

            //////////////////////////////////////////
            // find all of the indexes to add
            //////////////////////////////////////////

            auto currentIndexes = persistentTable->allIndexes();
            PersistentTable *deltaTable = persistentTable->deltaTable();

            {
                ConditionalExecuteWithMpMemory useMpMemoryIfReplicated(persistentTable->isCatalogTableReplicated());
                // iterate over indexes for this table in the catalog
                BOOST_FOREACH (LabeledIndex labeledIndex, catalogTable->indexes()) {
                    auto foundIndex = labeledIndex.second;
                    std::string indexName = foundIndex->name();
                    std::string catalogIndexId = TableCatalogDelegate::getIndexIdString(*foundIndex);

                    // Look for an index on the table to match the catalog index
                    bool found = false;
                    BOOST_FOREACH (TableIndex* currIndex, currentIndexes) {
                        std::string currentIndexId = currIndex->getId();
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
                        assert(index);
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
                BOOST_FOREACH (TableIndex* currIndex, currentIndexes) {
                    std::string currentIndexId = currIndex->getId();

                    bool found = false;
                    // iterate through all of the catalog indexes,
                    //  looking for a match.
                    BOOST_FOREACH (LabeledIndex labeledIndex, catalogTable->indexes()) {
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
                            BOOST_FOREACH (TableIndex* currDeltaIndex, currentDeltaIndexes) {
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

            std::vector<catalog::MaterializedViewInfo*> survivingInfos;
            std::vector<MaterializedViewTriggerForWrite*> survivingViews;
            std::vector<MaterializedViewTriggerForWrite*> obsoleteViews;

            const catalog::CatalogMap<catalog::MaterializedViewInfo> & views = catalogTable->views();
            MaterializedViewTriggerForWrite::segregateMaterializedViews(persistentTable->views(),
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
                // Use the now-current definiton of the target table, to be updated later, if needed.
                TableCatalogDelegate* targetDelegate = getTableDelegate(oldDestTable->name());
                PersistentTable* destTable = oldDestTable; // fallback value if not (yet) redefined.
                if (targetDelegate) {
                    auto newDestTable = targetDelegate->getPersistentTable();
                    if (newDestTable) {
                        destTable = newDestTable;
                    }
                }

                ConditionalExecuteWithMpMemory useMpMemoryIfReplicated(persistentTable->isCatalogTableReplicated());
                // This guards its destTable from accidental deletion with a refcount bump.
                MaterializedViewTriggerForWrite::build(persistentTable, destTable, currInfo);
                obsoleteViews.push_back(currView);
            }


            {
                ConditionalExecuteWithMpMemory useMpMemoryIfReplicated(persistentTable->isCatalogTableReplicated());
                BOOST_FOREACH (auto toDrop, obsoleteViews) {
                    persistentTable->dropMaterializedView(toDrop);
                }
            }
        }
    }

    BOOST_FOREACH (LabeledFunction labeledFunction, m_database->functions()) {
        auto catalogFunction = labeledFunction.second;
        UserDefinedFunctionInfo *info = new UserDefinedFunctionInfo();
        info->returnType = (ValueType) catalogFunction->returnType();
        catalog::CatalogMap<catalog::FunctionParameter> parameters = catalogFunction->parameters();
        info->paramTypes.resize(parameters.size());
        for (catalog::CatalogMap<catalog::FunctionParameter>::field_map_iter iter = parameters.begin();
                 iter != parameters.end(); iter++) {
            int key = std::stoi(iter->first);
            info->paramTypes[key] = (ValueType)iter->second->parameterType();
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

    assert(m_catalog != NULL); // the engine must be initialized
    VOLT_DEBUG("Updating catalog...");
    // apply the diff commands to the existing catalog
    // throws SerializeEEExceptions on error.
    m_catalog->execute(catalogPayload);

    // Set DR flag based on current catalog state
    auto catalogCluster = m_catalog->clusters().get("cluster");
    m_executorContext->drStream()->m_enabled = catalogCluster->drProducerEnabled();
    m_executorContext->drStream()->m_flushInterval = catalogCluster->drFlushInterval();
    if (m_executorContext->drReplicatedStream()) {
        m_executorContext->drReplicatedStream()->m_enabled = m_executorContext->drStream()->m_enabled;
        m_executorContext->drReplicatedStream()->m_flushInterval = m_executorContext->drStream()->m_flushInterval;
    }

    if (updateCatalogDatabaseReference() == false) {
        VOLT_ERROR("Error re-caching catalog references.");
        return false;
    }

    markAllExportingStreamsNew();

    std::map<std::string, ExportTupleStream*> purgedStreams;
    processCatalogDeletes(timestamp, false, purgedStreams);
    if (SynchronizedThreadLock::countDownGlobalTxnStartCount(m_isLowestSite)) {
        processReplicatedCatalogDeletes(timestamp, purgedStreams);
        SynchronizedThreadLock::signalLowestSiteFinished();
    }

    if (processCatalogAdditions(timestamp, false, isStreamUpdate, purgedStreams) == false) {
        VOLT_ERROR("Error processing catalog additions.");
        purgedStreams.clear();
        return false;
    }

    rebuildTableCollections();

    if (SynchronizedThreadLock::countDownGlobalTxnStartCount(m_isLowestSite)) {
        assert(SynchronizedThreadLock::isLowestSiteContext());
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
    if (SynchronizedThreadLock::countDownGlobalTxnStartCount(m_isLowestSite)) {
        SynchronizedThreadLock::signalLowestSiteFinished();
    }

    m_catalog->purgeDeletions();

    VOLT_DEBUG("Updated catalog...");
    return true;
}

void
VoltDBEngine::purgeMissingStreams(std::map<std::string, ExportTupleStream*> & purgedStreams) {
    BOOST_FOREACH (LabeledStreamWrapper entry, purgedStreams) {
        //Do delete later
        if (entry.second) {
            m_exportingStreams[entry.first] = NULL;
            m_exportingDeletedStreams[entry.first] = entry.second;
            entry.second->pushEndOfStream();
        }
    }
}

void
VoltDBEngine::markAllExportingStreamsNew() {
    //Mark all streams new so that schema is sent on next tuple.
    BOOST_FOREACH (LabeledStreamWrapper entry, m_exportingStreams) {
        if (entry.second != NULL) {
            entry.second->setNew();
        }
    }
}

bool
VoltDBEngine::loadTable(int32_t tableId,
                        ReferenceSerializeInputBE &serializeIn,
                        int64_t txnId, int64_t spHandle, int64_t lastCommittedSpHandle,
                        int64_t uniqueId,
                        bool returnConflictRows,
                        bool shouldDRStream,
                        int64_t undoToken) {
    //Not going to thread the unique id through.
    //The spHandle and lastCommittedSpHandle aren't really used in load table
    //since their only purpose as of writing this (1/2013) they are only used
    //for export data and we don't technically support loading into an export table
    setUndoToken(undoToken);
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(),
                                             txnId,
                                             spHandle,
                                             lastCommittedSpHandle,
                                             uniqueId,
                                             false);

    if (shouldDRStream) {
        m_executorContext->checkTransactionForDR();
    }

    Table* ret = getTableById(tableId);
    if (ret == NULL) {
        VOLT_ERROR("Table ID %d doesn't exist. Could not load data",
                   (int) tableId);
        return false;
    }

    PersistentTable* table = dynamic_cast<PersistentTable*>(ret);
    if (table == NULL) {
        VOLT_ERROR("Table ID %d(name '%s') is not a persistent table."
                   " Could not load data",
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
            (table->isCatalogTableReplicated(), isLowestSite(), &s_loadTableException, VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE);
    if (possiblySynchronizedUseMpMemory.okToExecute()) {
        try {
            table->loadTuplesForLoadTable(serializeIn,
                                          NULL,
                                          returnConflictRows ? &m_resultOutput : NULL,
                                          shouldDRStream,
                                          ExecutorContext::currentUndoQuantum() == NULL);
        }
        catch (const ConstraintFailureException &cfe) {
            s_loadTableException = VOLT_EE_EXCEPTION_TYPE_CONSTRAINT_VIOLATION;
            if (returnConflictRows) {
                // This should not happen because all errors are swallowed and constraint violations are returned
                // as failed rows in the result
                throw;
            }
            else {
                // pre-serialize the exception here since we need to cleanup tuple memory within this sync block
                resetReusedResultOutputBuffer();
                cfe.serialize(getExceptionOutputSerializer());
                return false;
            }
        }
        catch (const SQLException &sqe) {
            s_loadTableException = VOLT_EE_EXCEPTION_TYPE_SQL;
            throw;
        }
        catch (const SerializableEEException& serializableExc) {
            // Exceptions that are not constraint failures or sql exeception are treated as fatal.
            // This is legacy behavior.  Perhaps we cannot be ensured of data integrity for some mysterious
            // other kind of exception?
            s_loadTableException = serializableExc.getType();
            throwFatalException("%s", serializableExc.message().c_str());
        }

        if (table->isCatalogTableReplicated() && returnConflictRows) {
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
        s_loadTableException = VOLT_EE_EXCEPTION_TYPE_NONE;
    }
    else if (s_loadTableException == VOLT_EE_EXCEPTION_TYPE_CONSTRAINT_VIOLATION) {
        // An constraint failure exception was thrown on the lowest site thread and
        // handle it on the other threads too.
        if (!returnConflictRows) {
            std::ostringstream oss;
            oss << "Replicated load table failed (constraint violation) on other thread for table \""
                << table->name() << "\".\n";
            VOLT_DEBUG("%s", oss.str().c_str());
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE, oss.str().c_str());
        }
        // Offending rows will be serialized on lowest site thread.
        return false;
    }
    else if (s_loadTableException == VOLT_EE_EXCEPTION_TYPE_SQL) {
        // An sql exception was thrown on the lowest site thread and
        // handle it on the other threads too.
        std::ostringstream oss;
        oss << "Replicated load table failed (sql exception) on other thread for table \""
            << table->name() << "\".\n";
        VOLT_DEBUG("%s", oss.str().c_str());
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE, oss.str().c_str());
    }
    else if (s_loadTableException != VOLT_EE_EXCEPTION_TYPE_NONE) { // some other kind of exception occurred on lowest site thread
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
        m_tablesBySignatureHash.clear();

        // need to re-map all the table ids / indexes
        getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_TABLE);
        getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_INDEX);
    }

    // Walk through table delegates and update local table collections
    BOOST_FOREACH (LabeledTCD cd, m_catalogDelegates) {
        auto tcd = cd.second;
        assert(tcd);
        if (! tcd) {
            continue;
        }
        Table* localTable = tcd->getTable();
        assert(localTable);
        if (! localTable) {
            VOLT_ERROR("DEBUG-NULL: %s", cd.first.c_str());
            continue;
        }
        assert(m_database);
        auto catTable = m_database->tables().get(localTable->name());
        int32_t relativeIndexOfTable = catTable->relativeIndex();
        const std::string& tableName = tcd->getTable()->name();
        if (catTable->isreplicated()) {
            if (updateReplicated) {
                // This engine is responsible for updating the catalog table maps for all sites.
                ExecuteWithAllSitesMemory execAllSites;
                for (auto engineIt = execAllSites.begin(); engineIt != execAllSites.end(); ++engineIt) {
                    EngineLocals& curr = engineIt->second;
                    VoltDBEngine* currEngine = curr.context->getContextEngine();
                    SynchronizedThreadLock::assumeSpecificSiteContext(curr);
                    currEngine->m_tables[relativeIndexOfTable] = localTable;
                    currEngine->m_tablesByName[tableName] = localTable;
                }
            }
        }
        else if (! updateReplicated) {
            m_tables[relativeIndexOfTable] = localTable;
            m_tablesByName[tableName] = localTable;
        }

        TableStats* stats = NULL;
        PersistentTable* persistentTable = tcd->getPersistentTable();
        if (persistentTable) {
            stats = persistentTable->getTableStats();
            if (! tcd->materialized()) {
                int64_t hash = *reinterpret_cast<const int64_t*>(tcd->signatureHash());
                if (catTable->isreplicated()) {
                    if (updateReplicated) {
                        ExecuteWithAllSitesMemory execAllSites;
                        for (auto engineIt = execAllSites.begin(); engineIt != execAllSites.end(); ++engineIt) {
                            EngineLocals& curr = engineIt->second;
                            VoltDBEngine* currEngine = curr.context->getContextEngine();
                            SynchronizedThreadLock::assumeSpecificSiteContext(curr);
                            currEngine->m_tablesBySignatureHash[hash] = persistentTable;
                        }
                    }
                }
                else if (! updateReplicated) {
                    m_tablesBySignatureHash[hash] = persistentTable;
                }
            }

            // add all of the indexes to the stats source
            std::vector<TableIndex*> const& tindexes = persistentTable->allIndexes();
            if (catTable->isreplicated()) {
                if (updateReplicated) {
                    ExecuteWithAllSitesMemory execAllSites;
                    for (auto engineIt = execAllSites.begin(); engineIt != execAllSites.end(); ++engineIt) {
                        EngineLocals& curr = engineIt->second;
                        VoltDBEngine* currEngine = curr.context->getContextEngine();
                        SynchronizedThreadLock::assumeSpecificSiteContext(curr);
                        if (! fromScratch) {
                            // This is a swap or truncate and we need to clear the old index stats sources for this table
                            currEngine->getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_TABLE,
                                                                                relativeIndexOfTable);
                            currEngine->getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_INDEX,
                                                                                relativeIndexOfTable);
                        }
                        BOOST_FOREACH (auto index, tindexes) {
                            currEngine->getStatsManager().registerStatsSource(STATISTICS_SELECTOR_TYPE_INDEX,
                                                                              relativeIndexOfTable,
                                                                              index->getIndexStats());
                        }
                        currEngine->getStatsManager().registerStatsSource(STATISTICS_SELECTOR_TYPE_TABLE,
                                                                          relativeIndexOfTable,
                                                                          stats);
                    }
                }
            }
            else if (! updateReplicated) {
                if (! fromScratch) {
                    // This is a swap or truncate and we need to clear the old index stats sources for this table
                    getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_TABLE, relativeIndexOfTable);
                    getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_INDEX, relativeIndexOfTable);
                }
                BOOST_FOREACH (auto index, tindexes) {
                    getStatsManager().registerStatsSource(STATISTICS_SELECTOR_TYPE_INDEX,
                                                          relativeIndexOfTable,
                                                          index->getIndexStats());
                }
                getStatsManager().registerStatsSource(STATISTICS_SELECTOR_TYPE_TABLE,
                                                      relativeIndexOfTable,
                                                      stats);
            }
        }
        else {
            // Streamed tables are all partitioned.
            if (updateReplicated) {
                continue;
            }
            // Streamed tables could not be truncated or swapped, so we will never change this
            // table stats map in a not-from-scratch mode.
            // Before this rebuildTableCollections() is called, pre-built DR conflict tables
            // (which are also streamed tables) should have already been instantiated.
            if (fromScratch) {
                stats = tcd->getStreamedTable()->getTableStats();
                getStatsManager().registerStatsSource(STATISTICS_SELECTOR_TYPE_TABLE,
                                                      relativeIndexOfTable,
                                                      stats);
            }
        }
    }
    resetDRConflictStreamedTables();
}

void VoltDBEngine::swapDRActions(PersistentTable* table1, PersistentTable* table2) {
    TableCatalogDelegate* tcd1 = getTableDelegate(table1->name());
    TableCatalogDelegate* tcd2 = getTableDelegate(table2->name());
    assert(!tcd1->materialized());
    assert(!tcd2->materialized());
    // Point the Map from signature hash point to the correct persistent tables
    int64_t hash1 = *reinterpret_cast<const int64_t*>(tcd1->signatureHash());
    int64_t hash2 = *reinterpret_cast<const int64_t*>(tcd2->signatureHash());
    // Most swap action is already done.
    // But hash(tcd1) is still pointing to old persistent table1, which is now table2.
    assert(m_tablesBySignatureHash[hash1] == table2);
    assert(m_tablesBySignatureHash[hash2] == table1);
    m_tablesBySignatureHash[hash1] = table1;
    m_tablesBySignatureHash[hash2] = table2;
    table1->signature(tcd1->signatureHash());
    table2->signature(tcd2->signatureHash());

    // Generate swap table DREvent
    assert(table1->isDREnabled() == table2->isDREnabled());
    assert(table1->isDREnabled()); // This is checked before calling this method.
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
        m_executorContext->drStream()->generateDREvent(SWAP_TABLE, lastCommittedSpHandle,
                spHandle, uniqueId, payload);
    }
    if (m_executorContext->drReplicatedStream() && m_executorContext->drReplicatedStream()->drStreamStarted()) {
        m_executorContext->drReplicatedStream()->generateDREvent(SWAP_TABLE, lastCommittedSpHandle,
                spHandle, uniqueId, payload);
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
        assert(m_drPartitionedConflictStreamedTable == wellKnownTable);
        wellKnownTable = getTableByName(DR_REPLICATED_CONFLICT_TABLE_NAME);
        m_drReplicatedConflictStreamedTable =
            dynamic_cast<StreamedTable*>(wellKnownTable);
        assert(m_drReplicatedConflictStreamedTable == wellKnownTable);
    }
    else {
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
    }
    else {
        m_plans.reset(new EnginePlanSet());
    }

    PlanSet& plans = *m_plans;
    std::string plan = m_topend->planForFragmentId(fragId);
    if (plan.length() == 0) {
        char msg[1024];
        snprintf(msg, 1024, "Fetched empty plan from frontend for PlanFragment '%jd'",
                 (intmax_t)fragId);
        VOLT_ERROR("%s", msg);
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, msg);
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
    assert(m_currExecutorVec);
}

// -------------------------------------------------
// Initialization Functions
// -------------------------------------------------
// Parameter MATVIEW is the materialized view class,
// either MaterializedViewTriggerForStreamInsert for views on streams or
// MaterializedViewTriggerForWrite for views on persistent tables.
template <class MATVIEW>
static bool updateMaterializedViewDestTable(std::vector<MATVIEW*> & views,
                                              PersistentTable* target,
                                              catalog::MaterializedViewInfo* targetMvInfo) {
    std::string targetName = target->name();

    // find the materialized view that uses the table or its precursor (by the same name).
    BOOST_FOREACH(MATVIEW* currView, views) {
        PersistentTable* currTarget = currView->destTable();
        std::string currName = currTarget->name();
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
                                                               TABLE* storageTable,
                                                               bool updateReplicated) {
    // walk views
    VOLT_DEBUG("Processing views for table %s", storageTable->name().c_str());
    BOOST_FOREACH (LabeledView labeledView, catalogTable->views()) {
        auto catalogView = labeledView.second;
        catalog::Table const* destCatalogTable = catalogView->dest();
        int32_t catalogIndex = destCatalogTable->relativeIndex();
        auto destTable = static_cast<PersistentTable*>(m_tables[catalogIndex]);
        assert(destTable);
        VOLT_DEBUG("Updating view on table %s", destTable->name().c_str());
        assert(destTable == dynamic_cast<PersistentTable*>(m_tables[catalogIndex]));
        // Ensure that the materialized view controlling the existing
        // target table by the same name is using the latest version of
        // the table and view definition.
        // OR create a new materialized view link to connect the tables
        // if there is not one already with a matching target table name.
        ConditionalExecuteWithMpMemory useMpMemoryIfReplicated(updateReplicated);
        if ( ! updateMaterializedViewDestTable(storageTable->views(),
                                               destTable,
                                               catalogView)) {
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
            auto handler = new MaterializedViewHandler(destTable,
                                                       mvHandlerInfo,
                                                       mvHandlerInfo->groupByColumnCount(),
                                                       this);
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
            if (catalogTable->tuplelimitDeleteStmt().size() > 0) {
                auto stmt = catalogTable->tuplelimitDeleteStmt().begin()->second;
                std::string const& b64String = stmt->fragments().begin()->second->plannodetree();
                std::string jsonPlan = getTopend()->decodeBase64AndDecompress(b64String);
                ConditionalExecuteWithMpMemory useMpMemoryIfReplicated(updateReplicated);
                boost::shared_ptr<ExecutorVector> vec = ExecutorVector::fromJsonPlan(this,
                                                                                     jsonPlan,
                                                                                     -1);
                const std::vector<AbstractExecutor*>& execs = vec->getExecutorList();
                // Since purge executors are only called from insert, there is no need to coordinate
                // the execution of the delete across sites because we are already running on the lowest
                // site during insert::p_execute. Disabling the replicated flag will avoid the inner
                // coordination.
                BOOST_FOREACH(AbstractExecutor* exec, execs) {
                    exec->disableReplicatedFlag();
                }
                persistentTable->swapPurgeExecutorVector(vec);
            }
            else {
                ConditionalExecuteWithMpMemory useMpMemoryIfReplicated(updateReplicated);
                // get rid of the purge fragment from the persistent
                // table if it has been removed from the catalog
                boost::shared_ptr<ExecutorVector> nullPtr;
                persistentTable->swapPurgeExecutorVector(nullPtr);
            }
        }
        else {
            auto streamedTable = dynamic_cast<StreamedTable*>(table);
            assert(streamedTable);
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

/** Perform once per second, non-transactional work. */
void VoltDBEngine::tick(int64_t timeInMillis, int64_t lastCommittedSpHandle) {
    m_executorContext->setupForTick(lastCommittedSpHandle);
    //Push tuples for exporting streams.
    BOOST_FOREACH (LabeledStream table, m_exportingTables) {
        table.second->flushOldTuples(timeInMillis);
    }
    //On Tick do cleanup of dropped streams.
    BOOST_FOREACH (LabeledStreamWrapper entry, m_exportingDeletedStreams) {
        if (entry.second) {
            entry.second->periodicFlush(-1L, lastCommittedSpHandle);
            delete entry.second;
        }
    }
    m_exportingDeletedStreams.clear();

    m_executorContext->drStream()->periodicFlush(timeInMillis, lastCommittedSpHandle);
    if (m_executorContext->drReplicatedStream()) {
        m_executorContext->drReplicatedStream()->periodicFlush(timeInMillis, lastCommittedSpHandle);
    }
}

/** Bring the Export and DR system to a steady state with no pending committed data */
void VoltDBEngine::quiesce(int64_t lastCommittedSpHandle) {
    m_executorContext->setupForQuiesce(lastCommittedSpHandle);
    BOOST_FOREACH (LabeledStream table, m_exportingTables) {
        table.second->flushOldTuples(-1L);
    }
    //On quiesce do cleanup of dropped streams.
    BOOST_FOREACH (LabeledStreamWrapper entry, m_exportingDeletedStreams) {
        if (entry.second) {
            entry.second->periodicFlush(-1L, lastCommittedSpHandle);
            delete entry.second;
        }
    }
    m_exportingDeletedStreams.clear();
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

    BOOST_FOREACH (boost::shared_ptr<ExecutorVector> ev_guard, plans) {
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
        if ( ! getTableById(locator)) {
            char message[256];
            snprintf(message, 256,  "getStats() called with selector %d, and"
                    " an invalid locator %d that does not correspond to"
                    " a table", selector, locator);
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          message);
        }
        locatorIds.push_back(locator);
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
            char message[256];
            snprintf(message, 256, "getStats() called with an unrecognized selector"
                    " %d", selector);
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          message);
        }
    }
    catch (const SerializableEEException &e) {
        serializeException(e);
        return -1;
    }

    if (resultTable != NULL) {
        resultTable->serializeTo(m_resultOutput);
        m_resultOutput.writeIntAt(lengthPosition,
                                  static_cast<int32_t>(m_resultOutput.size()
                                                       - sizeof(int32_t)));
        return 1;
    }
    return 0;
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
        int64_t undoToken,
        ReferenceSerializeInputBE &serializeIn) {
    Table* found = getTableById(tableId);
    if (! found) {
        return false;
    }

    auto table = dynamic_cast<PersistentTable*>(found);
    if (table == NULL) {
        assert(table != NULL);
        return false;
    }
    setUndoToken(undoToken);

    // Crank up the necessary persistent table streaming mechanism(s).
    if (!table->activateStream(streamType, m_partitionId, tableId, serializeIn)) {
        return false;
    }

    // keep track of snapshotting tables. a table already in cow mode
    // can not be re-activated for cow mode.
    if (tableStreamTypeIsSnapshot(streamType)) {
        if (m_snapshottingTables.find(tableId) != m_snapshottingTables.end()) {
            assert(false);
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
                                               const TableStreamType streamType,
                                               ReferenceSerializeInputBE &serialInput) {
    int64_t remaining = TABLE_STREAM_SERIALIZATION_ERROR;
    try {
        std::vector<int> positions;
        remaining = tableStreamSerializeMore(tableId, streamType, serialInput, positions);
        if (remaining >= 0) {
            char *resultBuffer = getReusedResultBuffer();
            assert(resultBuffer != NULL);
            int resultBufferCapacity = getReusedResultBufferCapacity();
            if (resultBufferCapacity < sizeof(jint) * positions.size()) {
                throwFatalException("tableStreamSerializeMore: result buffer not large enough");
            }
            ReferenceSerializeOutput results(resultBuffer, resultBufferCapacity);
            // Write the array size as a regular integer.
            assert(positions.size() <= std::numeric_limits<int32_t>::max());
            results.writeInt((int32_t)positions.size());
            // Copy the position vector's contiguous storage to the returned results buffer.
            BOOST_FOREACH (int ipos, positions) {
                results.writeInt(ipos);
            }
        }
        VOLT_DEBUG("tableStreamSerializeMore: deserialized %d buffers, %ld remaining",
                   (int)positions.size(), (long)remaining);
    }
    catch (SerializableEEException &e) {
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

    if (!tableStreamTypeIsValid(streamType)) {
        // Failure
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
    PersistentTable *table = NULL;

    if (tableStreamTypeIsSnapshot(streamType)) {
        // If a completed table is polled, return 0 bytes serialized. The
        // Java engine will always poll a fully serialized table one more
        // time (it doesn't see the hasMore return code).  Note that the
        // dynamic cast was already verified in activateCopyOnWrite.
        table = findInMapOrNull(tableId, m_snapshottingTables);
        if (table == NULL) {
            return TABLE_STREAM_SERIALIZATION_ERROR;
        }

        remaining = table->streamMore(outputStreams, streamType, retPositions);
        if (remaining <= 0) {
            m_snapshottingTables.erase(tableId);
            table->decrementRefcount();
        }
    }
    else if (tableStreamTypeIsStreamIndexing(streamType)) {
        Table* found = getTableById(tableId);
        if (found == NULL) {
            return TABLE_STREAM_SERIALIZATION_ERROR;
        }

        PersistentTable* currentTable = dynamic_cast<PersistentTable*>(found);
        assert(currentTable != NULL);
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
    }
    else {
        Table* found = getTableById(tableId);
        if (found == NULL) {
            return TABLE_STREAM_SERIALIZATION_ERROR;
        }

        table = dynamic_cast<PersistentTable*>(found);
        remaining = table->streamMore(outputStreams, streamType, retPositions);
    }

    return remaining;
}

/*
 * Apply the updates in a recovery message.
 */
void VoltDBEngine::processRecoveryMessage(RecoveryProtoMsg *message) {
    CatalogId tableId = message->tableId();
    Table* found = getTableById(tableId);
    if (! found) {
        throwFatalException(
                "Attempted to process recovery message for tableId %d but the table could not be found", tableId);
    }
    PersistentTable *table = dynamic_cast<PersistentTable*>(found);
    assert(table);
    table->processRecoveryMessage(message, NULL);
}

int64_t VoltDBEngine::exportAction(bool syncAction,
                                   int64_t uso,
                                   int64_t seqNo,
                                   std::string tableSignature) {
    std::map<std::string, StreamedTable*>::iterator pos = m_exportingTables.find(tableSignature);

    // return no data and polled offset for unavailable tables.
    if (pos == m_exportingTables.end()) {
        // ignore trying to sync a non-exported table
        if (syncAction) {
            return 0;
        }

        m_resultOutput.writeInt(0);
        if (uso < 0) {
            return 0;
        }
        else {
            return uso;
        }
    }

    Table *table_for_el = pos->second;
    if (syncAction) {
        table_for_el->setExportStreamPositions(seqNo, (size_t) uso);
    }
    return 0;
}

void VoltDBEngine::getUSOForExportTable(size_t &ackOffset, int64_t &seqNo, std::string tableSignature) {

    // defaults mean failure
    ackOffset = 0;
    seqNo = -1;

    std::map<std::string, StreamedTable*>::iterator pos = m_exportingTables.find(tableSignature);

    // return no data and polled offset for unavailable tables.
    if (pos == m_exportingTables.end()) {
        return;
    }

    Table *table_for_el = pos->second;
    table_for_el->getExportStreamPositions(seqNo, ackOffset);
    return;
}

size_t VoltDBEngine::tableHashCode(int32_t tableId) {
    Table* found = getTableById(tableId);
    if (! found) {
        throwFatalException("Tried to calculate a hash code for a table that doesn't exist with id %d\n", tableId);
    }

    PersistentTable *table = dynamic_cast<PersistentTable*>(found);
    if (table == NULL) {
        throwFatalException(
                "Tried to calculate a hash code for a table that is not a persistent table id %d\n",
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
    boost::scoped_ptr<TheHashinator> hashinator_guard(hashinator);

    std::vector<int64_t> mispartitionedRowCounts;

    BOOST_FOREACH (CatalogId tableId, tableIds) {
        std::map<CatalogId, Table*>::iterator table = m_tables.find(tableId);
        if (table == m_tables.end()) {
            throwFatalException("Unknown table id %d", tableId);
        }
        Table* found = table->second;
        mispartitionedRowCounts.push_back(found->validatePartitioning(hashinator, m_partitionId));
    }

    m_resultOutput.writeInt(static_cast<int32_t>(sizeof(int64_t) * numTables));

    BOOST_FOREACH (int64_t mispartitionedRowCount, mispartitionedRowCounts) {
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
    }
    else {
        m_resultOutput.writeByte(static_cast<int8_t>(0));
    }
}

int64_t VoltDBEngine::applyBinaryLog(int64_t txnId,
                                  int64_t spHandle,
                                  int64_t lastCommittedSpHandle,
                                  int64_t uniqueId,
                                  int32_t remoteClusterId,
                                  int32_t remotePartitionId,
                                  int64_t undoToken,
                                  const char *log) {
    DRTupleStreamDisableGuard guard(m_executorContext, !m_isActiveActiveDREnabled);
    setUndoToken(undoToken);
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(),
                                             txnId,
                                             spHandle,
                                             lastCommittedSpHandle,
                                             uniqueId,
                                             false);
    // If using replicated stream (drProtocolVersion < NO_REPLICATED_STREAM_PROTOCOL_VERSION), coordinate on stream 16383
    // If not using replicated stream (drProtocolVersion >= NO_REPLICATED_STREAM_PROTOCOL_VERSION), coordinate on stream 0
    // Coordination is needed in both cases for replicate table.
    // The stream with replicated table changes will be executed firstly on lowest side. Then other sites took other changes.
    // The actual skip of replicated table DRRecord happens inside BinaryLogSink.

    // Notice: We now get the consumer drProtocolVersion from its producer side (m_drStream).
    // This assumes that all consumers use same protocol as its producer.
    // However, once we start supporting promote dr protocol (e.g. upgrade dr protocol via promote event from one coordinator cluster),
    // the coordinate cluster's consumer sides could operate in different protocols.
    // At that time, we need explicity pass in the consumer side protocol version for deciding weather
    // its corresponding remote producer has replicated stream or not.
    bool onLowestSite = false;
    int32_t replicatedTableStreamId = m_drStream->drProtocolVersion() < DRTupleStream::NO_REPLICATED_STREAM_PROTOCOL_VERSION ? 16383 : 0;
    if (UniqueId::isMpUniqueId(uniqueId) && (remotePartitionId == replicatedTableStreamId)) {
        VOLT_TRACE("applyBinaryLogMP for replicated table");
        onLowestSite = SynchronizedThreadLock::countDownGlobalTxnStartCount(isLowestSite());
    }
    int64_t rowCount = m_wrapper.apply(log, m_tablesBySignatureHash, &m_stringPool, this, remoteClusterId, uniqueId);
    if (onLowestSite) {
        SynchronizedThreadLock::signalLowestSiteFinished();
    }
    return rowCount;
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
        if (drProtocolVersion >= DRTupleStream::NO_REPLICATED_STREAM_PROTOCOL_VERSION &&
                m_drReplicatedStream != NULL) {
            delete m_drReplicatedStream;
            m_drReplicatedStream = NULL;
        }
        else if (drProtocolVersion < DRTupleStream::NO_REPLICATED_STREAM_PROTOCOL_VERSION &&
                m_drReplicatedStream == NULL && m_isLowestSite) {
            m_drReplicatedStream = new DRTupleStream(16383, m_drStream->m_defaultCapacity, drProtocolVersion);
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
        assert(uq);
        uq->registerUndoAction(
                new (*uq) ExecuteTaskUndoGenerateDREventAction(
                        m_executorContext->drStream(), m_executorContext->drReplicatedStream(),
                        m_executorContext->m_partitionId,
                        type, lastCommittedSpHandle,
                        spHandle, uniqueId, payloads));
        break;
    }
    default:
        throwFatalException("Unknown task type %d", taskType);
    }
}

void VoltDBEngine::executePurgeFragment(PersistentTable* table) {
    boost::shared_ptr<ExecutorVector> pev = table->getPurgeExecutorVector();

    // Push a new frame onto the stack for this executor vector
    // to report its tuples modified.  We don't want to actually
    // send this count back to the client---too confusing.  Just
    // throw it away.
    m_executorContext->pushNewModifiedTupleCounter();
    pev->setupContext(m_executorContext);

    try {
        m_executorContext->executeExecutors(0);
    }
    catch (const SerializableEEException &e) {
        // restore original DML statement state.
        m_currExecutorVec->setupContext(m_executorContext);
        m_executorContext->popModifiedTupleCounter();
        throw;
    }

    // restore original DML statement state.
    m_currExecutorVec->setupContext(m_executorContext);
    m_executorContext->popModifiedTupleCounter();
}

static std::string dummy_last_accessed_plan_node_name("no plan node in progress");

void VoltDBEngine::addToTuplesModified(int64_t amount) {
    m_executorContext->addToTuplesModified(amount);
}

void TempTableTupleDeleter::operator()(AbstractTempTable* tbl) const {
    if (tbl != NULL) {
        tbl->deleteAllTempTuples();
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
        int64_t dummyExceptionTracker = 0;
        ConditionalSynchronizedExecuteWithMpMemory possiblySynchronizedUseMpMemory(updateReplicated, m_isLowestSite, &dummyExceptionTracker, int64_t(-1));
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
                }
                if (persistentTable->isCatalogTableReplicated() != updateReplicated) {
                    VOLT_TRACE("[Partition %d] skip %s\n", m_partitionId, persistentTable->name().c_str());
                    continue;
                }
                if (persistentTable->materializedViewTrigger()) {
                    VOLT_TRACE("[Partition %d] %s->materializedViewTrigger()->setEnabled(%s)\n",
                               m_partitionId, persistentTable->name().c_str(), value?"true":"false");
                    // Single table view
                    persistentTable->materializedViewTrigger()->setEnabled(value);
                }
                else if (persistentTable->materializedViewHandler()) {
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

void VoltDBEngine::loadBuiltInJavaFunctions() {
    // Hard code the info of format_timestamp function
    UserDefinedFunctionInfo *info = new UserDefinedFunctionInfo();
    info->returnType = VALUE_TYPE_VARCHAR;
    info->paramTypes.resize(2);
    info->paramTypes.at(0) = VALUE_TYPE_TIMESTAMP;
    info->paramTypes.at(1) = VALUE_TYPE_VARCHAR;

    m_functionInfo[FUNC_VOLT_FORMAT_TIMESTAMP] = info;
}

} // namespace voltdb
