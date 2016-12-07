/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#include "catalog/catalog.h"
#include "catalog/catalogmap.h"
#include "catalog/cluster.h"
#include "catalog/column.h"
#include "catalog/columnref.h"
#include "catalog/connector.h"
#include "catalog/database.h"
#include "catalog/index.h"
#include "catalog/materializedviewhandlerinfo.h"
#include "catalog/materializedviewinfo.h"
#include "catalog/planfragment.h"
#include "catalog/statement.h"
#include "catalog/table.h"
#include "common/ElasticHashinator.h"
#include "common/executorcontext.hpp"
#include "common/FailureInjection.h"
#include "common/FatalException.hpp"
#include "common/LegacyHashinator.h"
#include "common/InterruptException.h"
#include "common/RecoveryProtoMessage.h"
#include "common/SerializableEEException.h"
#include "common/TupleOutputStream.h"
#include "common/TupleOutputStreamProcessor.h"
#include "executors/abstractexecutor.h"
#include "indexes/tableindex.h"
#include "indexes/tableindexfactory.h"
#include "plannodes/abstractplannode.h"
#include "plannodes/plannodefragment.h"
#include "storage/AbstractDRTupleStream.h"
#include "storage/tablefactory.h"
#include "storage/persistenttable.h"
#include "storage/streamedtable.h"
#include "storage/MaterializedViewHandler.h"
#include "storage/MaterializedViewTriggerForWrite.h"
#include "storage/TableCatalogDelegate.hpp"
#include "storage/CompatibleDRTupleStream.h"
#include "storage/DRTupleStream.h"
#include "org_voltdb_jni_ExecutionEngine.h" // to use static values

#include "boost/foreach.hpp"
#include "boost/scoped_ptr.hpp"
#include "boost/shared_ptr.hpp"
// The next #define limits the number of features pulled into the build
// We don't use those features.
#define BOOST_MULTI_INDEX_DISABLE_SERIALIZATION
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/sequenced_index.hpp>

#include <sstream>
#include <locale>
#include <typeinfo>

ENABLE_BOOST_FOREACH_ON_CONST_MAP(Column);
ENABLE_BOOST_FOREACH_ON_CONST_MAP(Index);
ENABLE_BOOST_FOREACH_ON_CONST_MAP(MaterializedViewInfo);
ENABLE_BOOST_FOREACH_ON_CONST_MAP(Table);

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

/// This class wrapper around a typedef allows forward declaration as in scoped_ptr<EnginePlanSet>.
class EnginePlanSet : public PlanSet { };

VoltDBEngine::VoltDBEngine(Topend *topend, LogProxy *logProxy)
    : m_currentIndexInBatch(-1),
      m_currentUndoQuantum(NULL),
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
      m_compatibleDRStream(NULL),
      m_compatibleDRReplicatedStream(NULL),
      m_currExecutorVec(NULL)
{
}

bool
VoltDBEngine::initialize(int32_t clusterIndex,
                         int64_t siteId,
                         int32_t partitionId,
                         int32_t hostId,
                         std::string hostname,
                         int32_t drClusterId,
                         int32_t defaultDrBufferSize,
                         int64_t tempTableMemoryLimit,
                         bool createDrReplicatedStream,
                         int32_t compactionThreshold)
{
    m_clusterIndex = clusterIndex;
    m_siteId = siteId;
    m_partitionId = partitionId;
    m_tempTableMemoryLimit = tempTableMemoryLimit;
    m_compactionThreshold = compactionThreshold;

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

    // configure DR stream and DR compatible stream
    m_drStream = new DRTupleStream(partitionId, defaultDrBufferSize);
    m_compatibleDRStream = new CompatibleDRTupleStream(partitionId, defaultDrBufferSize);
    if (createDrReplicatedStream) {
        m_drReplicatedStream = new DRTupleStream(16383, defaultDrBufferSize);
        m_compatibleDRReplicatedStream = new CompatibleDRTupleStream(16383, defaultDrBufferSize);
    }

    // set the DR version
    m_drVersion = DRTupleStream::PROTOCOL_VERSION;

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
    return true;
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

    BOOST_FOREACH (LabeledTCD cd, m_catalogDelegates) {
        delete cd.second;
    }

    BOOST_FOREACH (TID tid, m_snapshottingTables) {
        tid.second->decrementRefcount();
    }

    delete m_executorContext;

    delete m_drReplicatedStream;
    delete m_drStream;
    delete m_compatibleDRStream;
    delete m_compatibleDRReplicatedStream;
}

// ------------------------------------------------------------------
// OBJECT ACCESS FUNCTIONS
// ------------------------------------------------------------------
catalog::Catalog *VoltDBEngine::getCatalog() const {
    return (m_catalog.get());
}

Table* VoltDBEngine::getTable(int32_t tableId) const
{
    // Caller responsible for checking null return value.
    return findInMapOrNull(tableId, m_tables);
}

Table* VoltDBEngine::getTable(const std::string& name) const
{
    // Caller responsible for checking null return value.
    return findInMapOrNull(name, m_tablesByName);
}

TableCatalogDelegate* VoltDBEngine::getTableDelegate(const std::string& name) const
{
    // Caller responsible for checking null return value.
    return findInMapOrNull(name, m_delegatesByName);
}

catalog::Table* VoltDBEngine::getCatalogTable(const std::string& name) const {
    // iterate over all of the tables in the new catalog
    BOOST_FOREACH (LabeledTable labeledTable, m_database->tables()) {
        catalog::Table *catalogTable = labeledTable.second;
        if (catalogTable->name() == name) {
            return catalogTable;
        }
    }
    return NULL;
}

void VoltDBEngine::serializeTable(int32_t tableId, SerializeOutput& out) const
{
    // Just look in our list of tables
    Table* table = getTable(tableId);
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
 * @param serialize_in          A SerializeInput object containing the parameters.
 *                              The JNI call has an array of Java Objects.  These
 *                              have been serialized and stuffed into a byte buffer
 *                              which is shared between the EE and the JVM.  This
 *                              shared buffer is in the engine on the EE side, and
 *                              in a pool of ByteBuffers on the Java side.  The
 *                              Java byte buffer pools own these, but the EE can
 *                              use them.
 * @param txnId                 The transaction id.  This comes from the JNI call directly.
 * @param lastCommittedSpHandle The handle of the last committed single partition handle.
 *                              This is directly from the JNI call.
 * @param uniqueId              The unique id, taken directly from the JNI call.
 * @param undoToken             The undo token, taken directly from
 *                              the JNI call
 */
int VoltDBEngine::executePlanFragments(int32_t numFragments,
                                       int64_t planfragmentIds[],
                                       int64_t inputDependencyIds[],
                                       ReferenceSerializeInputBE &serialize_in,
                                       int64_t txnId,
                                       int64_t spHandle,
                                       int64_t lastCommittedSpHandle,
                                       int64_t uniqueId,
                                       int64_t undoToken)
{
    // count failures
    int failures = 0;

    setUndoToken(undoToken);

    // configure the execution context.
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(),
                                             txnId,
                                             spHandle,
                                             lastCommittedSpHandle,
                                             uniqueId);

    m_executorContext->checkTransactionForDR();

    // reset these at the start of each batch
    m_executorContext->m_progressStats.resetForNewBatch();
    NValueArray &params = m_executorContext->getParameterContainer();

    for (m_currentIndexInBatch = 0; m_currentIndexInBatch < numFragments; ++m_currentIndexInBatch) {

        int usedParamcnt = serialize_in.readShort();
        m_executorContext->setUsedParameterCount(usedParamcnt);
        if (usedParamcnt < 0) {
            throwFatalException("parameter count is negative: %d", usedParamcnt);
        }
        assert (usedParamcnt < MAX_PARAM_COUNT);

        for (int j = 0; j < usedParamcnt; ++j) {
            params[j].deserializeFromAllocateForStorage(serialize_in, &m_stringPool);
        }

        // success is 0 and error is 1.
        if (executePlanFragment(planfragmentIds[m_currentIndexInBatch],
                                inputDependencyIds ? inputDependencyIds[m_currentIndexInBatch] : -1,
                                m_currentIndexInBatch == 0,
                                m_currentIndexInBatch == (numFragments - 1))) {
            ++failures;
            break;
        }

        // at the end of each frag, rollup and reset counters
        m_executorContext->m_progressStats.rollUpForPlanFragment();

        m_stringPool.purge();
    }

    m_currentIndexInBatch = -1;

    return failures;
}

int VoltDBEngine::executePlanFragment(int64_t planfragmentId,
                                      int64_t inputDependencyId,
                                      bool first,
                                      bool last)
{
    assert(planfragmentId != 0);

    m_currentInputDepId = static_cast<int32_t>(inputDependencyId);

    /*
     * Reserve space in the result output buffer for the number of
     * result dependencies and for the dirty byte. Necessary for a
     * plan fragment because the number of produced depenencies may
     * not be known in advance.
     */
    if (first) {
        m_startOfResultBuffer = m_resultOutput.reserveBytes(sizeof(int32_t)
                                                            + sizeof(int8_t));
        m_dirtyFragmentBatch = false;
    }

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

    // write dirty-ness of the batch and number of dependencies output to the FRONT of
    // the result buffer
    if (last) {
        m_resultOutput.writeIntAt(m_startOfResultBuffer, static_cast<int32_t>(m_resultOutput.position() - m_startOfResultBuffer) - sizeof(int32_t) - sizeof(int8_t));
        m_resultOutput.writeBoolAt(m_startOfResultBuffer + sizeof(int32_t), m_dirtyFragmentBatch);
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

    VOLT_DEBUG("Finished executing successfully.");
    return result;
}

void VoltDBEngine::releaseUndoToken(int64_t undoToken)
{
    if (m_currentUndoQuantum != NULL && m_currentUndoQuantum->getUndoToken() == undoToken) {
        m_currentUndoQuantum = NULL;
        m_executorContext->setupForPlanFragments(NULL);
    }
    m_undoLog.release(undoToken);

    if (m_executorContext->drStream()) {
        m_executorContext->drStream()->endTransaction(m_executorContext->currentUniqueId());
    }
    if (m_executorContext->drReplicatedStream()) {
        m_executorContext->drReplicatedStream()->endTransaction(m_executorContext->currentUniqueId());
    }
}

void VoltDBEngine::undoUndoToken(int64_t undoToken)
{
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
bool VoltDBEngine::send(Table* dependency) {
    VOLT_DEBUG("Sending Dependency from C++");
    m_resultOutput.writeInt(-1); // legacy placeholder for old output id
    if (!dependency->serializeTo(m_resultOutput))
        return false;
    m_numResultDependencies++;
    return true;
}

int VoltDBEngine::loadNextDependency(Table* destination) {
    return m_topend->loadNextDependency(m_currentInputDepId, &m_stringPool, destination);
}

// -------------------------------------------------
// Catalog Functions
// -------------------------------------------------
bool VoltDBEngine::updateCatalogDatabaseReference() {
    catalog::Cluster *cluster = m_catalog->clusters().get("cluster");
    if (!cluster) {
        VOLT_ERROR("Unable to find cluster catalog information");
        return false;
    }

    m_database = cluster->databases().get("database");
    if (!m_database) {
        VOLT_ERROR("Unable to find database catalog information");
        return false;
    }
    m_isActiveActiveDREnabled = m_database->isActiveActiveDRed();

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

    assert(m_catalog != NULL);
    VOLT_DEBUG("Loading catalog...");


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

    // load up all the tables, adding all tables
    if (processCatalogAdditions(timestamp) == false) {
        return false;
    }

    rebuildTableCollections();

    // load up all the materialized views
    // and limit delete statements.
    //
    // This must be done after loading all the tables.
    initMaterializedViewsAndLimitDeletePlans();

    VOLT_DEBUG("Loaded catalog...");
    return true;
}

/*
 * Obtain the recent deletion list from the catalog.  For any item in
 * that list with a corresponding table delegate, process a deletion.
 *
 * TODO: This should be extended to find the parent delegate if the
 * deletion isn't a top-level object .. and delegates should have a
 * deleteChildCommand() intrface.
 *
 * Note, this only deletes tables, indexes are deleted in
 * processCatalogAdditions(..) for dumb reasons.
 */
void
VoltDBEngine::processCatalogDeletes(int64_t timestamp)
{
    std::vector<std::string> deletion_vector;
    m_catalog->getDeletedPaths(deletion_vector);
    std::set<std::string> deletions(deletion_vector.begin(), deletion_vector.end());

    // delete any empty persistent tables, forcing them to be rebuilt
    // (Unless the are actually being deleted -- then this does nothing)

    BOOST_FOREACH (LabeledTCD delegatePair, m_catalogDelegates) {
        TableCatalogDelegate *tcd = delegatePair.second;
        Table* table = tcd->getTable();

        // skip export tables for now
        StreamedTable *streamedtable = dynamic_cast<StreamedTable*>(table);
        if (streamedtable) {
            continue;
        }

        // identify empty tables and mark for deletion
        if (table->activeTupleCount() == 0) {
            deletions.insert(delegatePair.first);
        }
    }

    // delete tables in the set
    BOOST_FOREACH (std::string path, deletions) {
        VOLT_TRACE("delete path:");

        std::map<std::string, TableCatalogDelegate*>::iterator pos = m_catalogDelegates.find(path);
        if (pos == m_catalogDelegates.end()) {
           continue;
        }
        TableCatalogDelegate *tcd = pos->second;
        /*
         * Instruct the table to flush all export data
         * Then tell it about the new export generation/catalog txnid
         * which will cause it to notify the topend export data source
         * that no more data is coming for the previous generation
         */
        assert(tcd);
        if (tcd) {
            Table *table = tcd->getTable();
            m_delegatesByName.erase(table->name());
            StreamedTable *streamedtable = dynamic_cast<StreamedTable*>(table);
            if (streamedtable) {
                const std::string signature = tcd->signature();
                streamedtable->setSignatureAndGeneration(signature, timestamp);
                m_exportingTables.erase(signature);
            }
            delete tcd;
        }
        m_catalogDelegates.erase(pos);
    }
}

static bool haveDifferentSchema(catalog::Table *t1, voltdb::PersistentTable *t2)
{
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

        const TupleSchema::ColumnInfo *columnInfo = t2->schema()->getColumnInfo(index);

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
VoltDBEngine::processCatalogAdditions(int64_t timestamp)
{
    // iterate over all of the tables in the new catalog
    BOOST_FOREACH (LabeledTable labeledTable, m_database->tables()) {
        // get the catalog's table object
        catalog::Table *catalogTable = labeledTable.second;
        // get the delegate for the table... add the table if it's null
        TableCatalogDelegate* tcd = findInMapOrNull(catalogTable->path(), m_catalogDelegates);
        if (!tcd) {
            VOLT_TRACE("add a completely new table or rebuild an empty table...");

            //////////////////////////////////////////
            // add a completely new table
            //////////////////////////////////////////

            tcd = new TableCatalogDelegate(catalogTable->signature(),
                                           m_compactionThreshold);
            // use the delegate to init the table and create indexes n' stuff
            tcd->init(*m_database, *catalogTable);
            m_catalogDelegates[catalogTable->path()] = tcd;
            Table* table = tcd->getTable();
            m_delegatesByName[table->name()] = tcd;

            // set export info on the new table
            StreamedTable *streamedtable = tcd->getStreamedTable();
            if (streamedtable) {
                streamedtable->setSignatureAndGeneration(catalogTable->signature(), timestamp);
                m_exportingTables[catalogTable->signature()] = streamedtable;

                std::vector<catalog::MaterializedViewInfo*> survivingInfos;
                std::vector<MaterializedViewTriggerForStreamInsert*> survivingViews;
                std::vector<MaterializedViewTriggerForStreamInsert*> obsoleteViews;

                const catalog::CatalogMap<catalog::MaterializedViewInfo> & views = catalogTable->views();

                MaterializedViewTriggerForStreamInsert::segregateMaterializedViews(streamedtable->views(),
                        views.begin(), views.end(),
                        survivingInfos, survivingViews, obsoleteViews);

                for (int ii = 0; ii < survivingInfos.size(); ++ii) {
                    catalog::MaterializedViewInfo * currInfo = survivingInfos[ii];
                    PersistentTable* oldTargetTable = survivingViews[ii]->targetTable();
                    // Use the now-current definiton of the target table, to be updated later, if needed.
                    TableCatalogDelegate* targetDelegate =
                        dynamic_cast<TableCatalogDelegate*>(findInMapOrNull(oldTargetTable->name(),
                                                                            m_delegatesByName));
                    PersistentTable* targetTable = oldTargetTable; // fallback value if not (yet) redefined.
                    if (targetDelegate) {
                        PersistentTable* newTargetTable =
                            dynamic_cast<PersistentTable*>(targetDelegate->getTable());
                        if (newTargetTable) {
                            targetTable = newTargetTable;
                        }
                    }
                    // This guards its targetTable from accidental deletion with a refcount bump.
                    MaterializedViewTriggerForStreamInsert::build(streamedtable, targetTable, currInfo);
                    obsoleteViews.push_back(survivingViews[ii]);
                }

                BOOST_FOREACH (MaterializedViewTriggerForStreamInsert* toDrop, obsoleteViews) {
                    streamedtable->dropMaterializedView(toDrop);
                }

            }
        }
        else {

            //////////////////////////////////////////////
            // update the export info for existing tables
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
            StreamedTable *streamedTable = tcd->getStreamedTable();
            if (streamedTable) {
                streamedTable->setSignatureAndGeneration(catalogTable->signature(), timestamp);
                if (!tcd->exportEnabled()) {
                    // Evaluate export enabled or not and cache it on the tcd.
                    tcd->evaluateExport(*m_database, *catalogTable);
                    // If enabled hook up streamer
                    if (tcd->exportEnabled() && streamedTable->enableStream()) {
                        //Reset generation after stream wrapper is created.
                        streamedTable->setSignatureAndGeneration(catalogTable->signature(), timestamp);
                        m_exportingTables[catalogTable->signature()] = streamedTable;
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
                    catalog::MaterializedViewInfo * currInfo = survivingInfos[ii];
                    PersistentTable* oldTargetTable = survivingViews[ii]->targetTable();
                    // Use the now-current definiton of the target table, to be updated later, if needed.
                    TableCatalogDelegate* targetDelegate =
                        dynamic_cast<TableCatalogDelegate*>(findInMapOrNull(oldTargetTable->name(),
                                                                            m_delegatesByName));
                    PersistentTable* targetTable = oldTargetTable; // fallback value if not (yet) redefined.
                    if (targetDelegate) {
                        PersistentTable* newTargetTable =
                            dynamic_cast<PersistentTable*>(targetDelegate->getTable());
                        if (newTargetTable) {
                            targetTable = newTargetTable;
                        }
                    }
                    // This is not a leak -- the view metadata is self-installing into the new table.
                    // Also, it guards its targetTable from accidental deletion with a refcount bump.
                    MaterializedViewTriggerForStreamInsert::build(streamedTable, targetTable, currInfo);
                    obsoleteViews.push_back(survivingViews[ii]);
                }

                BOOST_FOREACH (MaterializedViewTriggerForStreamInsert * toDrop, obsoleteViews) {
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

                tcd->processSchemaChanges(*m_database, *catalogTable, m_delegatesByName);

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

            const std::vector<TableIndex*> currentIndexes = persistentTable->allIndexes();
            PersistentTable *deltaTable = NULL;
            if (persistentTable->deltaTable()) {
                deltaTable = static_cast<PersistentTable*>(persistentTable->deltaTable());
            }

            // iterate over indexes for this table in the catalog
            BOOST_FOREACH (LabeledIndex labeledIndex, catalogTable->indexes()) {
                catalog::Index* foundIndex = labeledIndex.second;
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
                    VOLT_TRACE("create and add the index...");
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
                catalog::MaterializedViewInfo * currInfo = survivingInfos[ii];
                PersistentTable* oldTargetTable = survivingViews[ii]->targetTable();
                // Use the now-current definiton of the target table, to be updated later, if needed.
                TableCatalogDelegate* targetDelegate = getTableDelegate(oldTargetTable->name());
                PersistentTable* targetTable = oldTargetTable; // fallback value if not (yet) redefined.
                if (targetDelegate) {
                    PersistentTable* newTargetTable =
                        dynamic_cast<PersistentTable*>(targetDelegate->getTable());
                    if (newTargetTable) {
                        targetTable = newTargetTable;
                    }
                }
                // This guards its targetTable from accidental deletion with a refcount bump.
                MaterializedViewTriggerForWrite::build(persistentTable, targetTable, currInfo);
                obsoleteViews.push_back(survivingViews[ii]);
            }

            BOOST_FOREACH (MaterializedViewTriggerForWrite* toDrop, obsoleteViews) {
                persistentTable->dropMaterializedView(toDrop);
            }
        }
    }

    // new plan fragments are handled differently.
    return true;
}


/*
 * Accept a list of catalog commands expressing a diff between the
 * current and the desired catalog. Execute those commands and create,
 * delete or modify the corresponding exectution engine objects.
 */
bool
VoltDBEngine::updateCatalog(const int64_t timestamp, const std::string &catalogPayload)
{
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
    catalog::Cluster* catalogCluster = m_catalog->clusters().get("cluster");
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

    processCatalogDeletes(timestamp);

    if (processCatalogAdditions(timestamp) == false) {
        VOLT_ERROR("Error processing catalog additions.");
        return false;
    }

    rebuildTableCollections();

    initMaterializedViewsAndLimitDeletePlans();

    m_catalog->purgeDeletions();
    VOLT_DEBUG("Updated catalog...");
    return true;
}

bool
VoltDBEngine::loadTable(int32_t tableId,
                        ReferenceSerializeInputBE &serializeIn,
                        int64_t txnId, int64_t spHandle, int64_t lastCommittedSpHandle,
                        int64_t uniqueId,
                        bool returnUniqueViolations,
                        bool shouldDRStream)
{
    //Not going to thread the unique id through.
    //The spHandle and lastCommittedSpHandle aren't really used in load table
    //since their only purpose as of writing this (1/2013) they are only used
    //for export data and we don't technically support loading into an export table
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(),
                                             txnId,
                                             spHandle,
                                             lastCommittedSpHandle,
                                             uniqueId);

    Table* ret = getTable(tableId);
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

    try {
        table->loadTuplesFrom(serializeIn, NULL, returnUniqueViolations ? &m_resultOutput : NULL, shouldDRStream);
    }
    catch (const SerializableEEException &e) {
        throwFatalException("%s", e.message().c_str());
    }
    return true;
}

/*
 * Delete and rebuild id based table collections. Does not affect
 * any currently stored tuples.
 */
void VoltDBEngine::rebuildTableCollections()
{
    // 1. See header comments explaining m_snapshottingTables.
    // 2. Don't clear m_exportTables. They are still exporting, even if deleted.
    // 3. Clear everything else.
    m_tables.clear();
    m_tablesByName.clear();
    m_tablesBySignatureHash.clear();

    // need to re-map all the table ids / indexes
    getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_TABLE);
    getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_INDEX);

    // walk the table delegates and update local table collections
    BOOST_FOREACH (LabeledTCD cd, m_catalogDelegates) {
        TableCatalogDelegate *tcd = cd.second;
        assert(tcd);
        if (!tcd) {
            continue;
        }
        Table* localTable = tcd->getTable();
        assert(localTable);
        if (!localTable) {
            VOLT_ERROR("DEBUG-NULL");//:%s", cd.first.c_str());
            std::cout << "DEBUG-NULL:" << cd.first << std::endl;
            continue;
        }
        assert(m_database);
        catalog::Table *catTable = m_database->tables().get(localTable->name());
        int32_t relativeIndexOfTable = catTable->relativeIndex();
        m_tables[relativeIndexOfTable] = localTable;
        m_tablesByName[tcd->getTable()->name()] = localTable;

        TableStats* stats;
        PersistentTable* persistentTable = tcd->getPersistentTable();
        if (persistentTable) {
            stats = persistentTable->getTableStats();
            if (!tcd->materialized()) {
                int64_t hash = *reinterpret_cast<const int64_t*>(tcd->signatureHash());
                m_tablesBySignatureHash[hash] = persistentTable;
            }

            // add all of the indexes to the stats source
            const std::vector<TableIndex*>& tindexes = persistentTable->allIndexes();
            BOOST_FOREACH (TableIndex *index, tindexes) {
                getStatsManager().registerStatsSource(STATISTICS_SELECTOR_TYPE_INDEX,
                                                      relativeIndexOfTable,
                                                      index->getIndexStats());
            }
        }
        else {
            stats = tcd->getStreamedTable()->getTableStats();
        }
        getStatsManager().registerStatsSource(STATISTICS_SELECTOR_TYPE_TABLE,
                                              relativeIndexOfTable,
                                              stats);

    }
    resetDRConflictStreamedTables();
}

void VoltDBEngine::resetDRConflictStreamedTables()
{
    if (getIsActiveActiveDREnabled()) {
        // These tables that go by well-known names SHOULD exist in an active-active
        // catalog except perhaps in some test scenarios with specialized (stubbed)
        // VoltDBEngine implementations, so avoid asserting that they exist.
        // DO assert that if the well-known streamed table exists, it has the right type.
        Table* wellKnownTable = getTable(DR_PARTITIONED_CONFLICT_TABLE_NAME);
        m_drPartitionedConflictStreamedTable =
            dynamic_cast<StreamedTable*>(wellKnownTable);
        assert(m_drPartitionedConflictStreamedTable == wellKnownTable);
        wellKnownTable = getTable(DR_REPLICATED_CONFLICT_TABLE_NAME);
        m_drReplicatedConflictStreamedTable =
            dynamic_cast<StreamedTable*>(wellKnownTable);
        assert(m_drReplicatedConflictStreamedTable == wellKnownTable);
    }
    else {
        m_drPartitionedConflictStreamedTable = NULL;
        m_drReplicatedConflictStreamedTable = NULL;
    }
}

void VoltDBEngine::setExecutorVectorForFragmentId(int64_t fragId)
{
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
static bool updateMaterializedViewTargetTable(std::vector<MATVIEW*> & views,
                                              PersistentTable* target,
                                              catalog::MaterializedViewInfo* targetMvInfo)
{
    std::string targetName = target->name();

    // find the materialized view that uses the table or its precursor (by the same name).
    BOOST_FOREACH(MATVIEW* currView, views) {
        PersistentTable* currTarget = currView->targetTable();
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
template<class TABLE> void VoltDBEngine::initMaterializedViews(catalog::Table *catalogTable,
                                                                        TABLE *table)
{
    // walk views
    BOOST_FOREACH (LabeledView labeledView, catalogTable->views()) {
        catalog::MaterializedViewInfo *catalogView = labeledView.second;
        const catalog::Table *destCatalogTable = catalogView->dest();
        int32_t catalogIndex = destCatalogTable->relativeIndex();
        PersistentTable *destTable = static_cast<PersistentTable*>(m_tables[catalogIndex]);
        assert(destTable);
        assert(destTable == dynamic_cast<PersistentTable*>(m_tables[catalogIndex]));
        // Ensure that the materialized view controlling the existing
        // target table by the same name is using the latest version of
        // the table and view definition.
        // OR create a new materialized view link to connect the tables
        // if there is not one already with a matching target table name.
        if ( ! updateMaterializedViewTargetTable(table->views(),
                                                 destTable,
                                                 catalogView)) {
            // This is a new view, a connection needs to be made using a new MaterializedViewTrigger..
            TABLE::MatViewType::build(table, destTable, catalogView);
        }
    }

    catalog::MaterializedViewHandlerInfo *mvHandlerInfo = catalogTable->mvHandlerInfo().get("mvHandlerInfo");
    // If the table has a mvHandlerInfo, it means this table is a target table of materialized view.
    // Now we only use the new view handler mechanism for joined table views.
    // Further code refactoring saved for future.
    if (mvHandlerInfo) {
        PersistentTable *destTable = static_cast<PersistentTable*>(m_tables[catalogTable->relativeIndex()]);
        if ( ! destTable->materializedViewHandler() || destTable->materializedViewHandler()->isDirty() ) {
            // The newly-added handler will at the same time trigger
            // the uninstallation of the previous (if exists) handler.
            MaterializedViewHandler *handler = new MaterializedViewHandler(destTable, mvHandlerInfo, this);
            if (destTable->isPersistentTableEmpty()) {
                handler->catchUpWithExistingData(this, false);
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
void VoltDBEngine::initMaterializedViewsAndLimitDeletePlans() {
    // walk tables
    BOOST_FOREACH (LabeledTable labeledTable, m_database->tables()) {
        catalog::Table *catalogTable = labeledTable.second;
        Table *table = m_tables[catalogTable->relativeIndex()];
        PersistentTable *persistentTable = dynamic_cast<PersistentTable*>(table);
        if (persistentTable != NULL) {
            initMaterializedViews(catalogTable, persistentTable);
            if (catalogTable->tuplelimitDeleteStmt().size() > 0) {
                catalog::Statement* stmt = catalogTable->tuplelimitDeleteStmt().begin()->second;
                const std::string b64String = stmt->fragments().begin()->second->plannodetree();
                std::string jsonPlan = getTopend()->decodeBase64AndDecompress(b64String);
                persistentTable->swapPurgeExecutorVector(ExecutorVector::fromJsonPlan(this,
                                                                                jsonPlan,
                                                                                -1));
            }
            else {
                // get rid of the purge fragment from the persistent
                // table if it has been removed from the catalog
                boost::shared_ptr<ExecutorVector> nullPtr;
                persistentTable->swapPurgeExecutorVector(nullPtr);
            }
        }
        else {
            StreamedTable *streamedTable = dynamic_cast<StreamedTable*>(table);
            assert(streamedTable);
            initMaterializedViews(catalogTable, streamedTable);
        }
    }
}

int VoltDBEngine::getResultsSize() const {
    return static_cast<int>(m_resultOutput.size());
}

void VoltDBEngine::setBuffers(char *parameterBuffer, int parameterBuffercapacity,
        char *resultBuffer, int resultBufferCapacity,
        char *exceptionBuffer, int exceptionBufferCapacity) {
    m_parameterBuffer = parameterBuffer;
    m_parameterBufferCapacity = parameterBuffercapacity;

    m_reusedResultBuffer = resultBuffer;
    m_reusedResultCapacity = resultBufferCapacity;

    m_exceptionBuffer = exceptionBuffer;
    m_exceptionBufferCapacity = exceptionBufferCapacity;
}

// -------------------------------------------------
// MISC FUNCTIONS
// -------------------------------------------------

bool VoltDBEngine::isLocalSite(const NValue& value) const
{
    int32_t index = m_hashinator->hashinate(value);
    return index == m_partitionId;
}

int32_t VoltDBEngine::getPartitionForPkHash(const int32_t pkHash) const
{
    return m_hashinator->partitionForToken(pkHash);
}

bool VoltDBEngine::isLocalSite(const int32_t pkHash) const
{
    return getPartitionForPkHash(pkHash) == m_partitionId;
}

std::string VoltDBEngine::dumpCurrentHashinator() const {
    return m_hashinator.get()->debug();
}

typedef std::pair<std::string, StreamedTable*> LabeledStream;

/** Perform once per second, non-transactional work. */
void VoltDBEngine::tick(int64_t timeInMillis, int64_t lastCommittedSpHandle) {
    m_executorContext->setupForTick(lastCommittedSpHandle);
    BOOST_FOREACH (LabeledStream table, m_exportingTables) {
        table.second->flushOldTuples(timeInMillis);
    }
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
    m_executorContext->drStream()->periodicFlush(-1L, lastCommittedSpHandle);
    if (m_executorContext->drReplicatedStream()) {
        m_executorContext->drReplicatedStream()->periodicFlush(-1L, lastCommittedSpHandle);
    }
}

std::string VoltDBEngine::debug(void) const
{
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
                           bool interval, int64_t now)
{
    Table *resultTable = NULL;
    std::vector<CatalogId> locatorIds;

    for (int ii = 0; ii < numLocators; ii++) {
        CatalogId locator = static_cast<CatalogId>(locators[ii]);
        locatorIds.push_back(locator);
    }
    size_t lengthPosition = m_resultOutput.reserveBytes(sizeof(int32_t));

    try {
        switch (selector) {
        case STATISTICS_SELECTOR_TYPE_TABLE:
            for (int ii = 0; ii < numLocators; ii++) {
                CatalogId locator = static_cast<CatalogId>(locators[ii]);
                if ( ! getTable(locator)) {
                    char message[256];
                    snprintf(message, 256,  "getStats() called with selector %d, and"
                            " an invalid locator %d that does not correspond to"
                            " a table", selector, locator);
                    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                                  message);
                }
            }

            resultTable = m_statsManager.getStats(
                (StatisticsSelectorType) selector,
                locatorIds, interval, now);
            break;
        case STATISTICS_SELECTOR_TYPE_INDEX:
            for (int ii = 0; ii < numLocators; ii++) {
                CatalogId locator = static_cast<CatalogId>(locators[ii]);
                if ( ! getTable(locator)) {
                    char message[256];
                    snprintf(message, 256,  "getStats() called with selector %d, and"
                            " an invalid locator %d that does not correspond to"
                            " a table", selector, locator);
                    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                                  message);
                }
            }

            resultTable = m_statsManager.getStats(
                (StatisticsSelectorType) selector,
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


void VoltDBEngine::setCurrentUndoQuantum(voltdb::UndoQuantum* undoQuantum)
{
    m_currentUndoQuantum = undoQuantum;
    m_executorContext->setupForPlanFragments(m_currentUndoQuantum);
}


/*
 * Exists to transition pre-existing unit test cases.
 */
void VoltDBEngine::updateExecutorContextUndoQuantumForTest()
{
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
    Table* found = getTable(tableId);
    if (! found) {
        return false;
    }

    PersistentTable *table = dynamic_cast<PersistentTable*>(found);
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
                                               ReferenceSerializeInputBE &serialize_in)
{
    int64_t remaining = TABLE_STREAM_SERIALIZATION_ERROR;
    try {
        std::vector<int> positions;
        remaining = tableStreamSerializeMore(tableId, streamType, serialize_in, positions);
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
        std::vector<int> &retPositions)
{
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
    else if (tableStreamTypeAppliesToPreTruncateTable(streamType)) {
        Table* found = getTable(tableId);
        if (found == NULL) {
            return TABLE_STREAM_SERIALIZATION_ERROR;
        }

        PersistentTable * currentTable = dynamic_cast<PersistentTable*>(found);
        assert(currentTable != NULL);
        // The ongoing TABLE STREAM needs the original table from the first table truncate.
        PersistentTable * originalTable = currentTable->currentPreTruncateTable();

        VOLT_DEBUG("tableStreamSerializeMore: type %s, rewinds to the table before the first truncate",
                tableStreamTypeToString(streamType).c_str());

        remaining = originalTable->streamMore(outputStreams, streamType, retPositions);
        if (remaining <= 0) {
            // The on going TABLE STREAM of the original table before the first table truncate has finished.
            // Reset all the previous table pointers to be NULL.
            currentTable->unsetPreTruncateTable();
            VOLT_DEBUG("tableStreamSerializeMore: type %s, null the previous truncate table pointer",
                    tableStreamTypeToString(streamType).c_str());
        }
    }
    else {
        Table* found = getTable(tableId);
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
    Table* found = getTable(tableId);
    if (! found) {
        throwFatalException(
                "Attempted to process recovery message for tableId %d but the table could not be found", tableId);
    }
    PersistentTable *table = dynamic_cast<PersistentTable*>(found);
    assert(table);
    table->processRecoveryMessage(message, NULL);
}

int64_t
VoltDBEngine::exportAction(bool syncAction, int64_t ackOffset, int64_t seqNo, std::string tableSignature)
{
    std::map<std::string, StreamedTable*>::iterator pos = m_exportingTables.find(tableSignature);

    // return no data and polled offset for unavailable tables.
    if (pos == m_exportingTables.end()) {
        // ignore trying to sync a non-exported table
        if (syncAction) {
            return 0;
        }

        m_resultOutput.writeInt(0);
        if (ackOffset < 0) {
            return 0;
        }
        else {
            return ackOffset;
        }
    }

    Table *table_for_el = pos->second;
    if (syncAction) {
        table_for_el->setExportStreamPositions(seqNo, (size_t) ackOffset);
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
    Table* found = getTable(tableId);
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

void VoltDBEngine::updateHashinator(HashinatorType type, const char *config, int32_t *configPtr, uint32_t numTokens) {
    switch (type) {
    case HASHINATOR_LEGACY:
        setHashinator(LegacyHashinator::newInstance(config));
        break;
    case HASHINATOR_ELASTIC:
        setHashinator(ElasticHashinator::newInstance(config, configPtr, numTokens));
        break;
    default:
        throwFatalException("Unknown hashinator type %d", type);
        break;
    }
}

void VoltDBEngine::dispatchValidatePartitioningTask(ReferenceSerializeInputBE &taskInfo) {
    std::vector<CatalogId> tableIds;
    const int32_t numTables = taskInfo.readInt();
    for (int ii = 0; ii < numTables; ii++) {
        tableIds.push_back(static_cast<int32_t>(taskInfo.readLong()));
    }

    HashinatorType type = static_cast<HashinatorType>(taskInfo.readInt());
    const char *config = taskInfo.getRawPointer();
    TheHashinator* hashinator;
    switch(type) {
        case HASHINATOR_LEGACY:
            hashinator = LegacyHashinator::newInstance(config);
            break;
        case HASHINATOR_ELASTIC:
            hashinator = ElasticHashinator::newInstance(config, NULL, 0);
            break;
        default:
            throwFatalException("Unknown hashinator type %d", type);
            break;
    }
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
    m_resultOutput.writeInt(m_drVersion);
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
                                  int64_t undoToken,
                                  const char *log) {
    DRTupleStreamDisableGuard guard(m_executorContext, !m_isActiveActiveDREnabled);
    setUndoToken(undoToken);
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(),
                                             txnId,
                                             spHandle,
                                             lastCommittedSpHandle,
                                             uniqueId);

    int64_t rowCount = m_wrapper.apply(log, m_tablesBySignatureHash, &m_stringPool, this, remoteClusterId);
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
        uint32_t drVersion = taskInfo.readInt();
        if (drVersion != DRTupleStream::PROTOCOL_VERSION) {
            m_executorContext->setDrStream(m_compatibleDRStream);
            if (m_compatibleDRReplicatedStream) {
                m_executorContext->setDrReplicatedStream(m_compatibleDRReplicatedStream);
            }
        }
        else {
            m_executorContext->setDrStream(m_drStream);
            if (m_drReplicatedStream) {
                m_executorContext->setDrReplicatedStream(m_drReplicatedStream);
            }
        }
        m_drVersion = drVersion;
        m_resultOutput.writeInt(0);
        break;
    }
    case TASK_TYPE_GENERATE_DR_EVENT: {
        // we start using in-band CATALOG_UPDATE at version 5
        if (m_drVersion >= 5) {
            DREventType type = (DREventType)taskInfo.readInt();
            int64_t uniqueId = taskInfo.readLong();
            int64_t lastCommittedSpHandle = taskInfo.readLong();
            int64_t spHandle = taskInfo.readLong();
            ByteArray payloads = taskInfo.readBinaryString();

            m_executorContext->drStream()->generateDREvent(type, lastCommittedSpHandle,
                    spHandle, uniqueId, payloads);
            if (m_executorContext->drReplicatedStream()) {
                m_executorContext->drReplicatedStream()->generateDREvent(type, lastCommittedSpHandle,
                        spHandle, uniqueId, payloads);
            }
        }
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

void TempTableTupleDeleter::operator()(TempTable* tbl) const {
    if (tbl != NULL) {
        tbl->deleteAllTempTuples();
    }
}

} // namespace voltdb
