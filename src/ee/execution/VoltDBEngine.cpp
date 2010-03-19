/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
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

#include <iostream>
#include <stdio.h>
#include <fstream>
#include <errno.h>
#include <sstream>
#include <unistd.h>
#include <locale>
#include "boost/shared_array.hpp"
#include "boost/scoped_array.hpp"
#include "boost/foreach.hpp"
#include "boost/scoped_ptr.hpp"
#include "VoltDBEngine.h"
#include "common/common.h"
#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/valuevector.h"
#include "common/TheHashinator.h"
#include "common/DummyUndoQuantum.hpp"
#include "common/tabletuple.h"
#include "common/executorcontext.hpp"
#include "common/CatalogUtil.h"
#include "catalog/catalogmap.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/site.h"
#include "catalog/partition.h"
#include "catalog/database.h"
#include "catalog/table.h"
#include "catalog/index.h"
#include "catalog/column.h"
#include "catalog/columnref.h"
#include "catalog/procedure.h"
#include "catalog/statement.h"
#include "catalog/planfragment.h"
#include "catalog/constraint.h"
#include "catalog/materializedviewinfo.h"
#include "catalog/connector.h"
#include "plannodes/abstractplannode.h"
#include "plannodes/abstractscannode.h"
#include "plannodes/nodes.h"
#include "plannodes/plannodeutil.h"
#include "plannodes/plannodefragment.h"
#include "executors/executors.h"
#include "executors/executorutil.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "indexes/tableindex.h"
#include "storage/constraintutil.h"
#include "storage/persistenttable.h"
#include "storage/MaterializedViewMetadata.h"
#include "org_voltdb_jni_ExecutionEngine.h" // to use static values
#include "stats/StatsAgent.h"
#include "voltdbipc.h"
#include "common/FailureInjection.h"

using namespace catalog;
using namespace std;

namespace voltdb {

const int64_t AD_HOC_FRAG_ID = -1;

VoltDBEngine::VoltDBEngine(Topend *topend, LogProxy *logProxy)
    : m_currentUndoQuantum(NULL),
      m_staticParams(MAX_PARAM_COUNT),
      m_currentOutputDepId(-1),
      m_currentInputDepId(-1),
      m_isELEnabled(false),
      m_stringPool(16777216, 2),
      m_numResultDependencies(0),
      m_logManager(logProxy),
      m_templateSingleLongTable(NULL),
      m_topend(topend)
{
    m_currentUndoQuantum = new DummyUndoQuantum();

    // init the number of planfragments executed
    m_pfCount = 0;

    // require a site id, at least, to inititalize.
    m_executorContext = NULL;
}

bool VoltDBEngine::initialize(
        int32_t clusterIndex,
        int32_t siteId,
        int32_t partitionId,
        int32_t hostId,
        std::string hostname) {
    // Be explicit about running in the standard C locale for now.
    std::locale::global(std::locale("C"));

    m_clusterIndex = clusterIndex;
    m_siteId = siteId;
    m_partitionId = partitionId;

    // Instantiate our catalog - it will be populated later on by load()
    m_catalog = boost::shared_ptr<catalog::Catalog>(new catalog::Catalog());

    // create the template single long (int) table
    assert (m_templateSingleLongTable == NULL);
    m_templateSingleLongTable = new char[m_templateSingleLongTableSize];
    memset(m_templateSingleLongTable, 0, m_templateSingleLongTableSize);
    m_templateSingleLongTable[7] = 23; // table size
    m_templateSingleLongTable[9] = 7; // size of header
    m_templateSingleLongTable[11] = 1; // number of columns
    m_templateSingleLongTable[12] = VALUE_TYPE_BIGINT; // column type
    m_templateSingleLongTable[13] = 0; // column name length
    m_templateSingleLongTable[20] = 1; // row count
    m_templateSingleLongTable[22] = 10; // row size

    // required for catalog loading.
    m_executorContext = new ExecutorContext(siteId,
                                            m_partitionId,
                                            m_currentUndoQuantum,
                                            getTopend(),
                                            m_isELEnabled,
                                            0, /* epoch not yet known */
                                            hostname,
                                            hostId);
    return true;
}

VoltDBEngine::~VoltDBEngine() {
    // WARNING WARNING WARNING
    // The sequence below in which objects are cleaned up/deleted is
    // fragile.  Reordering or adding additional destruction below
    // greatly increases the risk of accidentally freeing the same
    // object multiple times.  Change at your own risk.
    // --izzy 8/19/2009

    // Get rid of any dummy undo quantum first so m_undoLog.clear()
    // doesn't wipe this out before we do it.
    if (m_currentUndoQuantum != NULL && m_currentUndoQuantum->isDummy()) {
        delete m_currentUndoQuantum;
    }

    // Clear the undo log before deleting the persistent tables so
    // that the persistent table schema are still around so we can
    // actually find the memory that has been allocated to non-inlined
    // strings and deallocated it.
    m_undoLog.clear();

    for (int ii = 0; ii < m_tables.size(); ii++) {
        // this cleanup anti-pattern is a mixed-bag. It is on one hand
        // annoying that table cleanup is not self sufficient; on the
        // other hand, releasing managed buffers requires the VoltDBEngine
        // interface and coupling the storage component to the whole
        // topend is a testing burden.
        if (m_tables[ii])
            m_tables[ii]->cleanupManagedBuffers(m_topend);

        delete m_tables[ii];
    }

    for (int ii = 0; ii < m_planFragments.size(); ii++) {
        delete m_planFragments[ii];
    }

    // clean up memory for the template memory for the single long (int) table
    if (m_templateSingleLongTable)
        delete[] m_templateSingleLongTable;

    delete m_topend;
    delete m_executorContext;
}

// ------------------------------------------------------------------
// OBJECT ACCESS FUNCTIONS
// ------------------------------------------------------------------
catalog::Catalog *VoltDBEngine::getCatalog() const {
    return (m_catalog.get());
}

Table* VoltDBEngine::getTable(int32_t tableId) const {
    // Caller responsible for checking null return value.
    std::map<int32_t, Table*>::const_iterator lookup =
        m_tables.find(tableId);
    if (lookup != m_tables.end()) {
        return lookup->second;
    }
    return NULL;
}

Table* VoltDBEngine::getTable(std::string name) const {
    // Caller responsible for checking null return value.
    std::map<std::string, Table*>::const_iterator lookup =
        m_tablesByName.find(name);
    if (lookup != m_tablesByName.end()) {
        return lookup->second;
    }
    return NULL;
}

bool VoltDBEngine::serializeTable(int32_t tableId, SerializeOutput* out) const {
    // Just look in our list of tables
    std::map<int32_t, Table*>::const_iterator lookup =
        m_tables.find(tableId);
    if (lookup != m_tables.end()) {
        Table* table = lookup->second;
        table->serializeTo(*out);
        return true;
    } else {
        VOLT_ERROR("Unable to find table for TableId '%d'", (int) tableId);
        return false;
    }
}

// ------------------------------------------------------------------
// EXECUTION FUNCTIONS
// ------------------------------------------------------------------
int VoltDBEngine::executeQuery(int64_t planfragmentId, int32_t outputDependencyId, int32_t inputDependencyId,
                               const NValueArray &params,
                               int64_t txnId, int64_t lastCommittedTxnId,
                               bool first, bool last)
{
    m_currentOutputDepId = outputDependencyId;
    m_currentInputDepId = inputDependencyId;

    /*
     * Reserve space in the result output buffer for the number of
     * result dependencies and for the dirty byte. Necessary for a
     * plan fragment because the number of produced depenencies may
     * not be known in advance.
     */
    if (first) {
        m_startOfResultBuffer = m_resultOutput.reserveBytes(sizeof(int32_t) + sizeof(int8_t));
        m_dirtyFragmentBatch = false;
    }

    // set this to zero for dml operations
    m_tuplesModified = 0;

    /*
     * Reserve space in the result output buffer for the number of
     * result dependencies generated by this particular plan fragment.
     * Necessary for a plan fragment because the
     * number of produced depenencies may not be known in advance.
     */
    m_numResultDependencies = 0;
    std::size_t numResultDependenciesCountOffset = m_resultOutput.reserveBytes(4);

    // configure the execution context.
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(),
                                             txnId,
                                             lastCommittedTxnId);

   // count the number of plan fragments executed
    ++m_pfCount;

    // execution lists for planfragments are cached by planfragment id
    assert (planfragmentId >= -1);
    //printf("Looking to execute fragid %jd\n", (intmax_t)planfragmentId);
    std::map<int64_t, boost::shared_ptr<ExecutorVector> >::const_iterator iter = m_executorMap.find(planfragmentId);
    assert (iter != m_executorMap.end());
    boost::shared_ptr<ExecutorVector> execsForFrag = iter->second;

    // Walk through the queue and execute each plannode.  The query
    // planner guarantees that for a given plannode, all of its
    // children are positioned before it in this list, therefore
    // dependency tracking is not needed here.
    size_t ttl = execsForFrag->list.size();
    for (int ctr = 0; ctr < ttl; ++ctr) {
        AbstractExecutor *executor = execsForFrag->list[ctr];
        assert (executor);

        try {
            // Now call the execute method to actually perform whatever action it is that
            // the node is supposed to do...
            if (!executor->execute(params)) {
                VOLT_TRACE("The Executor's execution at position '%d'"
                         " failed for PlanFragment '%d'",
                         ctr, planfragmentId);
                // set these back to -1 for error handling
                m_currentOutputDepId = -1;
                m_currentInputDepId = -1;
                return ENGINE_ERRORCODE_ERROR;
            }
        } catch (SerializableEEException &e) {
            VOLT_TRACE("The Executor's execution at position '%d'"
                     " failed for PlanFragment '%d'",
                     ctr, planfragmentId);
            resetReusedResultOutputBuffer();
            e.serialize(getExceptionOutputSerializer());

            // set these back to -1 for error handling
            m_currentOutputDepId = -1;
            m_currentInputDepId = -1;
            return ENGINE_ERRORCODE_ERROR;
        }
    }
    for (int ctr = 0; ctr < ttl; ++ctr) {
        try {
            // Now call the postExecute so the executors can clean up if necessary
            execsForFrag->list[ctr]->postExecute();
        } catch (SerializableEEException &e) {
            VOLT_TRACE("The Executor's execution at position '%d'"
                       " failed for PlanFragment '%d' while running postExecute",
                       ctr, planfragmentId);
            resetReusedResultOutputBuffer();
            e.serialize(getExceptionOutputSerializer());

            // set these back to -1 for error handling
            m_currentOutputDepId = -1;
            m_currentInputDepId = -1;
            return ENGINE_ERRORCODE_ERROR;
        }
    }

    // assume this is sendless dml
    if (m_numResultDependencies == 0) {
        // put the number of tuples modified into our simple table
        uint64_t changedCount = htonll(m_tuplesModified);
        memcpy(m_templateSingleLongTable + m_templateSingleLongTableSize - 8, &changedCount, sizeof(changedCount));
        m_resultOutput.writeBytes(m_templateSingleLongTable, m_templateSingleLongTableSize);
        m_numResultDependencies++;
    }

    //Write the number of result dependencies if necessary.
    m_resultOutput.writeIntAt(numResultDependenciesCountOffset, m_numResultDependencies);

    // if a fragment modifies any tuples, the whole batch is dirty
    if (m_tuplesModified > 0)
        m_dirtyFragmentBatch = true;

    // write dirty-ness of the batch and number of dependencies output to the FRONT of
    // the result buffer
    if (last) {
        m_resultOutput.writeIntAt(m_startOfResultBuffer,
            static_cast<int32_t>((m_resultOutput.position() - m_startOfResultBuffer) - sizeof(int32_t)));
        m_resultOutput.writeBoolAt(m_startOfResultBuffer + sizeof(int32_t), m_dirtyFragmentBatch);
    }

    // set these back to -1 for error handling
    m_currentOutputDepId = -1;
    m_currentInputDepId = -1;

    VOLT_DEBUG("Finished executing.");
    return ENGINE_ERRORCODE_SUCCESS;
}

/*
 * Execute the supplied fragment in the context of the specified
 * cluster and database with the supplied parameters as arguments. A
 * catalog with all the necessary tables needs to already have been
 * loaded.
 */
int VoltDBEngine::executePlanFragment(std::string fragmentString, int32_t outputDependencyId, int32_t inputDependencyId,
                                      int64_t txnId,
                                      int64_t lastCommittedTxnId)
{
    int retval = ENGINE_ERRORCODE_ERROR;

    m_currentOutputDepId = outputDependencyId;
    m_currentInputDepId = inputDependencyId;

    // how many current plans (too see if we added any)
    size_t frags = m_planFragments.size();

    boost::scoped_array<char> buffer(new char[fragmentString.size() * 2 + 1]);
    catalog::Catalog::hexEncodeString(fragmentString.c_str(), buffer.get());
    std::string hexEncodedFragment(buffer.get());

    try
    {
        if (initPlanFragment(AD_HOC_FRAG_ID, hexEncodedFragment))
        {
            voltdb::NValueArray parameterValueArray(0);
            retval = executeQuery(AD_HOC_FRAG_ID, outputDependencyId, inputDependencyId, parameterValueArray,
                                  txnId, lastCommittedTxnId, true, true);
        }
        else
        {
            VOLT_ERROR("Unable to load ad-hoc plan fragment.");
        }
    }
    catch (SerializableEEException &e)
    {
        VOLT_TRACE("executePlanFragment: failed to initialize "
                   "ad-hoc plan fragment");
        resetReusedResultOutputBuffer();
        e.serialize(getExceptionOutputSerializer());
        retval = ENGINE_ERRORCODE_ERROR;
    }

    // clean up stuff
    m_executorMap.erase(AD_HOC_FRAG_ID);

    // delete any generated plan
    size_t nowFrags = m_planFragments.size();
    if (nowFrags > frags) {
        assert ((nowFrags - frags) == 1);
        delete m_planFragments.back();
        m_planFragments.pop_back();
    }

    // set these back to -1 for error handling
    m_currentOutputDepId = -1;
    m_currentInputDepId = -1;

    return retval;
}

// -------------------------------------------------
// RESULT FUNCTIONS
// -------------------------------------------------
bool VoltDBEngine::send(Table* dependency) {
    VOLT_DEBUG("Sending Dependency '%d' from C++", m_currentOutputDepId);
    m_resultOutput.writeInt(m_currentOutputDepId);
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
bool VoltDBEngine::loadCatalog(const std::string &catalogPayload) {
    assert(m_catalog != NULL); // the engine must be initialized
    VOLT_DEBUG("Loading catalog...");
    //cout << catalogPayload << endl;
    // TODO : how do we treat an error of catalog loading?

    m_catalog->execute(catalogPayload);

    // get a reference to the database and cluster
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

     // initialize the list of partition ids
    bool success = initCluster(cluster);
    if (success == false) {
        VOLT_ERROR("Unable to load partition list for cluster");
        return false;
    }

    // Tables care about EL state.
    if (m_database->connectors().size() > 0 &&
        m_database->connectors().get("0")->enabled())
    {
        VOLT_DEBUG("EL enabled.");
        m_executorContext->m_eltEnabled = true;
        m_isELEnabled = true;
    }

    // Loop through all the tables...
    std::map<std::string, catalog::Table*>::const_iterator table_iterator;
    for (table_iterator = m_database->tables().begin(); table_iterator != m_database->tables().end(); table_iterator++) {
        if (!initTable(m_database->relativeIndex(), table_iterator->second)) {
            VOLT_ERROR("Failed to initialize table '%s' from catalogs\n", table_iterator->second->name().c_str());
            return false;
        }
    }

    // load up all the materialized views
    initMaterializedViews();

    // load the plan fragments from the catalog
    clearAndLoadAllPlanFragments();

    // deal with the epoch
    int64_t epoch = cluster->localepoch() * (int64_t)1000;
    m_executorContext->setEpoch(epoch);

    VOLT_DEBUG("Loaded catalog...");
    return true;
}

bool VoltDBEngine::updateCatalog(const std::string &catalogPayload) {
    assert(m_catalog != NULL); // the engine must be initialized
    VOLT_DEBUG("Updating catalog...");

    // apply the diff commands to the existing catalog
    try {
        m_catalog->execute(catalogPayload);
    }
    catch (std::string s) {
        VOLT_ERROR("Error updating catalog: %s", s.c_str());
        return false;
    }

    // cache the database in m_database
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

    // this does the actual scary bits of reinitializing plan fragments
    if (!clearAndLoadAllPlanFragments()) {
        VOLT_ERROR("Error updating catalog planfragments");
        return false;
    }

    VOLT_DEBUG("Updated catalog...");
    return true;
}

bool VoltDBEngine::loadTable(bool allowELT, int32_t tableId,
                             ReferenceSerializeInput &serializeIn,
                             int64_t txnId, int64_t lastCommittedTxnId)
{
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(),
                                             txnId,
                                             lastCommittedTxnId);

    Table* ret = getTable(tableId);
    if (ret == NULL) {
        VOLT_ERROR("Table ID %d doesn't exist. Could not load data", (int) tableId);
        return false;
    }
    PersistentTable* table = dynamic_cast<PersistentTable*>(ret);
    if (table == NULL) {
        VOLT_ERROR("Table ID %d(name '%s') is not a persistent table. Could not load data",
                   (int) tableId, ret->name().c_str());
        return false;
    }

    table->loadTuplesFrom(allowELT, serializeIn);
    return true;
}

bool VoltDBEngine::clearAndLoadAllPlanFragments() {
    // clear the existing stuff if this is being called as part of a catalog change
    for (int ii = 0; ii < m_planFragments.size(); ii++)
        delete m_planFragments[ii];
    m_planFragments.clear();
    m_executorMap.clear();

    // initialize all the planfragments.
    std::map<std::string, catalog::Procedure*>::const_iterator proc_iterator;
    for (proc_iterator = m_database->procedures().begin(); proc_iterator != m_database->procedures().end(); proc_iterator++) {

        // Procedure
        const catalog::Procedure *catalog_proc = proc_iterator->second;
        VOLT_DEBUG("proc: %s", catalog_proc->name().c_str());
        std::map<std::string, catalog::Statement*>::const_iterator stmt_iterator;
        for (stmt_iterator = catalog_proc->statements().begin(); stmt_iterator != catalog_proc->statements().end(); stmt_iterator++) {

            // PlanFragment
            const catalog::Statement *catalogStmt = stmt_iterator->second;
            VOLT_DEBUG("  stmt: %s : %s", catalogStmt->name().c_str(), catalogStmt->sqltext().c_str());

            std::map<std::string, catalog::PlanFragment*>::const_iterator pf_iterator;
            for (pf_iterator = catalogStmt->fragments().begin(); pf_iterator!= catalogStmt->fragments().end(); pf_iterator++) {
                //int64_t fragId = uniqueIdForFragment(pf_iterator->second);
                int64_t fragId = uniqueIdForFragment(pf_iterator->second);
                std::string planNodeTree = pf_iterator->second->plannodetree();
                if (!initPlanFragment(fragId, planNodeTree)) {
                    VOLT_ERROR("Failed to initialize plan fragment '%s' from catalogs", pf_iterator->second->name().c_str());
                    VOLT_ERROR("Failed SQL Statement: %s", catalogStmt->sqltext().c_str());
                    return false;
                }
            }
        }
    }

    return true;
}

// -------------------------------------------------
// Initialization Functions
// -------------------------------------------------
bool VoltDBEngine::initPlanFragment(const int64_t fragId, const std::string planNodeTree) {

    // Deserialize the PlanFragment and stick in our local map

    std::map<int64_t, boost::shared_ptr<ExecutorVector> >::const_iterator iter = m_executorMap.find(fragId);
    if (iter != m_executorMap.end()) {
        VOLT_ERROR("Duplicate PlanNodeList entry for PlanFragment '%jd' during initialization", (intmax_t)fragId);
        return false;
    }

    // catalog method plannodetree returns PlanNodeList.java
    PlanNodeFragment *pnf = PlanNodeFragment::createFromCatalog(planNodeTree, m_database);
    m_planFragments.push_back(pnf);
    VOLT_TRACE("\n%s\n", pnf->debug().c_str());
    assert(pnf->getRootNode());

    if (!pnf->getRootNode()) {
        VOLT_ERROR("Deserialized PlanNodeFragment for PlanFragment '%jd' "
                 "does not have a root PlanNode", (intmax_t)fragId);
        return false;
    }

    boost::shared_ptr<ExecutorVector> ev = boost::shared_ptr<ExecutorVector>(new ExecutorVector());
    ev->tempTableMemoryInBytes = 0;

    // Initialize each node!
    for (int ctr = 0, cnt = (int)pnf->getExecuteList().size(); ctr < cnt; ctr++) {
        if (!initPlanNode(fragId, pnf->getExecuteList()[ctr], &(ev->tempTableMemoryInBytes))) {
            VOLT_ERROR("Failed to initialize PlanNode '%s' at position '%d' for PlanFragment '%jd'",
                     pnf->getExecuteList()[ctr]->debug().c_str(), ctr, (intmax_t)fragId);
            return false;
        }
    }

    // Initialize the vector of executors for this planfragment, used at runtime.
    for (int ctr = 0, cnt = (int)pnf->getExecuteList().size(); ctr < cnt; ctr++) {
        ev->list.push_back(pnf->getExecuteList()[ctr]->getExecutorRaw());
    }
    m_executorMap[fragId] = ev;

    return true;
}

bool VoltDBEngine::initPlanNode(const int64_t fragId, AbstractPlanNode* node, int* tempTableMemoryInBytes) {
    assert(node);
    assert(node->getExecutor() == NULL);

    // Executor is created here. An executor is *devoted* to this plannode
    // so that it can cache anything for the plannode
    AbstractExecutor* executor = getNewExecutor(this, node);
    assert(executor);
    node->setExecutor(executor);

    // If this PlanNode has an internal PlanNode (e.g., AbstractScanPlanNode can have internal Projections), then
    // we need to make sure that we set that internal node's executor as well
    if (node->getInlinePlanNodes().size() > 0) {
        std::map<PlanNodeType, AbstractPlanNode*>::iterator internal_it;
        for (internal_it = node->getInlinePlanNodes().begin(); internal_it != node->getInlinePlanNodes().end(); internal_it++) {
            AbstractPlanNode* inline_node = internal_it->second;
            if (!initPlanNode(fragId, inline_node, tempTableMemoryInBytes)) {
                VOLT_ERROR("Failed to initialize the internal PlanNode '%s' of PlanNode '%s'", inline_node->debug().c_str(), node->debug().c_str());
                return false;
            }
        }
    }

    // Now use the executor to initialize the plannode for execution later on
    if (!executor->init(this, m_database, tempTableMemoryInBytes)) {
        VOLT_ERROR("The Executor failed to initialize PlanNode '%s' for PlanFragment '%jd'", node->debug().c_str(), (intmax_t)fragId);
        return false;
    }

    return true;
}

bool VoltDBEngine::initTable(const int32_t databaseId, const catalog::Table *catalogTable) {
    assert(catalogTable);

    // Create a persistent table for this table in our catalog
    int32_t table_id = catalogTable->relativeIndex();
    if (m_tables.find(table_id) != m_tables.end()) {
        VOLT_ERROR("Duplicate table '%d' during initialization", table_id);
        return false;
    }
    if (m_tablesByName.find(catalogTable->name()) != m_tablesByName.end()) {
        VOLT_ERROR("Duplicate table '%s' during initialization", catalogTable->name().c_str());
        return false;
    }

    // Columns:
    // Column is stored as map<String, Column*> in Catalog. We have to
    // sort it by Column index to preserve column order.
    const int numColumns = static_cast<int>(catalogTable->columns().size());
    std::vector<voltdb::ValueType> columnTypes(numColumns);
    std::vector<uint16_t> columnLengths(numColumns);
    std::vector<bool> columnAllowNull(numColumns);
    std::map<std::string, catalog::Column*>::const_iterator col_iterator;
    std::string *columnNames = new std::string[numColumns];
    for (col_iterator = catalogTable->columns().begin(); col_iterator != catalogTable->columns().end(); col_iterator++) {
        const catalog::Column *catalog_column = col_iterator->second;
        const int columnIndex = catalog_column->index();
        const voltdb::ValueType type = static_cast<voltdb::ValueType>(catalog_column->type());
        columnTypes[columnIndex] = type;
        const uint16_t size = static_cast<uint16_t>(catalog_column->size());
        const uint16_t length = type == VALUE_TYPE_VARCHAR ? size : static_cast<uint16_t>(NValue::getTupleStorageSize(type));//Strings length is provided, other lengths are derived from type
        columnLengths[columnIndex] = length;
        columnAllowNull[columnIndex] = catalog_column->nullable();
        columnNames[catalog_column->index()] = catalog_column->name();
    }

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, true);
    //cout << "Created schema for " << catalogTable->name() << endl << schema->debug();
    // Indexes
    std::map<std::string, TableIndexScheme> index_map;
    std::map<std::string, catalog::Index*>::const_iterator idx_iterator;
    for (idx_iterator = catalogTable->indexes().begin(); idx_iterator != catalogTable->indexes().end(); idx_iterator++) {
        catalog::Index *catalog_index = idx_iterator->second;
        std::vector<int> index_columns;
        std::vector<ValueType> column_types;

        // The catalog::Index object now has a list of columns that are to be used
        if (catalog_index->columns().size() == (size_t)0) {
            VOLT_ERROR("Index '%s' in table '%s' does not declare any columns to use", catalog_index->name().c_str(), catalogTable->name().c_str());
            return false;
        }

        // Since the columns are not going to come back in the proper order from the catalogs, we'll use
        // the index attribute to make sure we put them in the right order
        index_columns.resize(catalog_index->columns().size());
        column_types.resize(catalog_index->columns().size());
        bool isIntsOnly = true;
        std::map<std::string, catalog::ColumnRef*>::const_iterator colref_iterator;
        for (colref_iterator = catalog_index->columns().begin(); colref_iterator != catalog_index->columns().end(); colref_iterator++) {
            catalog::ColumnRef *catalog_colref = colref_iterator->second;
            if (catalog_colref->index() < 0) {
                VOLT_ERROR("Invalid column '%d' for index '%s' in table '%s'", catalog_colref->index(), catalog_index->name().c_str(), catalogTable->name().c_str());
                return false;
            }
            // check if the column does not have an int type
            if ((catalog_colref->column()->type() != VALUE_TYPE_TINYINT) &&
                (catalog_colref->column()->type() != VALUE_TYPE_SMALLINT) &&
                (catalog_colref->column()->type() != VALUE_TYPE_INTEGER) &&
                (catalog_colref->column()->type() != VALUE_TYPE_BIGINT))
                isIntsOnly = false;
            index_columns[catalog_colref->index()] = catalog_colref->column()->index();
            column_types[catalog_colref->index()] = (ValueType) catalog_colref->column()->type();
        }

        TableIndexScheme index_scheme(catalog_index->name(), (TableIndexType)catalog_index->type(), index_columns, column_types, catalog_index->unique(), isIntsOnly, schema);
        index_map[catalog_index->name()] = index_scheme;
    }

    // Constraints
    std::string pkey_index_id;
    std::map<std::string, catalog::Constraint*>::const_iterator constraint_iterator;
    for (constraint_iterator = catalogTable->constraints().begin(); constraint_iterator != catalogTable->constraints().end(); constraint_iterator++) {
        catalog::Constraint *catalog_constraint = constraint_iterator->second;

        // Constraint Type
        ConstraintType type = (ConstraintType)catalog_constraint->type();
        switch (type) {
            case CONSTRAINT_TYPE_PRIMARY_KEY:
                // Make sure we have an index to use
                if (catalog_constraint->index() == NULL) {
                    VOLT_ERROR("The '%s' constraint '%s' on table '%s' does not specify an index", constraintutil::getTypeName(type).c_str(), catalog_constraint->name().c_str(), catalogTable->name().c_str());
                    return false;

                }
                // Make sure they didn't declare more than one primary key index
                else if (pkey_index_id.size() > 0) {
                    VOLT_ERROR("Trying to declare a primary key on table '%s' using index '%s' but '%s' was already set as the primary key",
                               catalogTable->name().c_str(), catalog_constraint->index()->name().c_str(), pkey_index_id.c_str());
                    return false;
                }
                pkey_index_id = catalog_constraint->index()->name();
                break;
            case CONSTRAINT_TYPE_UNIQUE:
                // Make sure we have an index to use
                // TODO: In the future I would like bring back my Constraint object so that we can keep
                //       track of everything that a table has...
                if (catalog_constraint->index() == NULL) {
                    VOLT_ERROR("The '%s' constraint '%s' on table '%s' does not specify an index", constraintutil::getTypeName(type).c_str(), catalog_constraint->name().c_str(), catalogTable->name().c_str());
                    return false;
                }
                break;
            // Unsupported
            case CONSTRAINT_TYPE_CHECK:
            case CONSTRAINT_TYPE_FOREIGN_KEY:
            case CONSTRAINT_TYPE_MAIN:
                VOLT_WARN("Unsupported type '%s' for constraint '%s'", constraintutil::getTypeName(type).c_str(), catalog_constraint->name().c_str());
                break;
            // Unknown
            default:
                VOLT_ERROR("Invalid constraint type '%s' for '%s'", constraintutil::getTypeName(type).c_str(), catalog_constraint->name().c_str());
                return false;
        }
    }

    // Build the index array
    std::vector<TableIndexScheme> indexes;
    TableIndexScheme pkey_index;
    std::map<std::string, TableIndexScheme>::const_iterator index_iterator;
    for (index_iterator = index_map.begin(); index_iterator != index_map.end(); index_iterator++) {

        // Exclude the primary key
        if (index_iterator->first.compare(pkey_index_id) == 0) {
            pkey_index = index_iterator->second;

        // Just add it to the list
        } else {
            indexes.push_back(index_iterator->second);
        }
    }

    // partition column:
    const catalog::Column* partitionColumn = catalogTable->partitioncolumn();
    int partitionColumnIndex = -1;
    if (partitionColumn != NULL)
        partitionColumnIndex = partitionColumn->index();

    voltdb::Table* table;
    if (pkey_index_id.size() == 0) {
        table = voltdb::TableFactory::getPersistentTable(databaseId, table_id, m_executorContext,
                                                         catalogTable->name(), schema, columnNames,
                                                         indexes, partitionColumnIndex,
                                                         isExportEnabledForTable(m_database, table_id),
                                                         isTableExportOnly(m_database, table_id));
    } else {
        table = voltdb::TableFactory::getPersistentTable(databaseId, table_id, m_executorContext,
                                                         catalogTable->name(), schema, columnNames,
                                                         pkey_index, indexes, partitionColumnIndex,
                                                         isExportEnabledForTable(m_database, table_id),
                                                         isTableExportOnly(m_database, table_id));
    }
    assert(table != NULL);
    getStatsManager().registerStatsSource(STATISTICS_SELECTOR_TYPE_TABLE, table_id, table->getTableStats());
    m_tables[table_id] = table;
    m_tablesByName[table->name()] = table;

    delete[] columnNames;
    return true;
}

bool VoltDBEngine::initMaterializedViews() {

    std::map<PersistentTable*, std::vector<MaterializedViewMetadata*> > allViews;

    // build all the materialized view metadata structure
    // start by iterating over all the tables in the catalog
    std::map<std::string, catalog::Table*>::const_iterator tableIterator;
    for (tableIterator = m_database->tables().begin(); tableIterator != m_database->tables().end(); tableIterator++) {
        catalog::Table *srcCatalogTable = tableIterator->second;
        PersistentTable *srcTable = dynamic_cast<PersistentTable*>(m_tables[srcCatalogTable->relativeIndex()]);

        // for each table look for any materialized views and iterate over them
        std::map<std::string, catalog::MaterializedViewInfo*>::const_iterator matviewIterator;
        for (matviewIterator = srcCatalogTable->views().begin(); matviewIterator != srcCatalogTable->views().end(); matviewIterator++) {
            catalog::MaterializedViewInfo *catalogView = matviewIterator->second;

            const catalog::Table *destCatalogTable = catalogView->dest();
            PersistentTable *destTable = dynamic_cast<PersistentTable*>(m_tables[destCatalogTable->relativeIndex()]);

            MaterializedViewMetadata *mvmd = new MaterializedViewMetadata(srcTable, destTable, catalogView);

            allViews[srcTable].push_back(mvmd);
        }
    }

    // get the lists of views for each table and stick them in there
    std::map<PersistentTable*, std::vector<MaterializedViewMetadata*> >::const_iterator viewListIterator;
    for (viewListIterator = allViews.begin(); viewListIterator != allViews.end(); viewListIterator++) {
        PersistentTable *srcTable = viewListIterator->first;
        srcTable->setMaterializedViews(viewListIterator->second);
    }

    return true;
}

bool VoltDBEngine::initCluster(const catalog::Cluster *catalogCluster) {

    // Find the partition id for this execution site.
    std::map<std::string, catalog::Site*>::const_iterator site_it;
    for (site_it = catalogCluster->sites().begin();
         site_it != catalogCluster->sites().end();
         site_it++)
    {
        catalog::Site *site = site_it->second;
        assert (site);
        std::string sname = site->name();
        if (atoi(sname.c_str()) == m_siteId) {
            assert(site->partition());
            std::string pname = site->partition()->name();
            m_partitionId = atoi(pname.c_str());
            break;
        }
    }
    // need to update executor context as partitionId wasn't
    // available when the structure was initially created.
    m_executorContext->m_partitionId = m_partitionId;
    m_totalPartitions = catalogCluster->partitions().size();
    return true;
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
// EL/BUFFER FUNCTIONS
// -------------------------------------------------
void VoltDBEngine::handoffReadyELBuffer(char* bufferPtr, int32_t bytesUsed, int32_t tableId) {
    assert(bufferPtr);
    assert(bytesUsed > 0);
    assert(tableId > 0);
    m_topend->handoffReadyELBuffer( bufferPtr, bytesUsed, tableId);
}

char* VoltDBEngine::claimManagedBuffer(int32_t desiredSizeInBytes) {
    assert(desiredSizeInBytes > 0);
    return m_topend->claimManagedBuffer(desiredSizeInBytes);
}

void VoltDBEngine::releaseManagedBuffer(char* bufferPtr) {
    assert(bufferPtr);
    m_topend->releaseManagedBuffer(bufferPtr);
}

// -------------------------------------------------
// MISC FUNCTIONS
// -------------------------------------------------

void VoltDBEngine::printReport() {
    std::cout << "==========" << std::endl;
    std::cout << "Report for Planfragment # " << m_pfCount << std::endl;
    typedef std::pair<int32_t, voltdb::Table*> TablePair;
    BOOST_FOREACH (TablePair table, m_tables) {
        std::vector<TableIndex*> indexes = table.second->allIndexes();
        if (!indexes.empty()) continue;
        BOOST_FOREACH (TableIndex *index, indexes) {
            index->printReport();
        }
    }
    std::cout << "==========" << std::endl << std::endl;
}

bool VoltDBEngine::isLocalSite(int64_t value) {
    int index = TheHashinator::hashinate(value, m_totalPartitions);
    return index == m_partitionId;
}

bool VoltDBEngine::isLocalSite(char *string, int32_t length) {
    int index = TheHashinator::hashinate(string, length, m_totalPartitions);
    return index == m_partitionId;
}

/** Perform once per second, non-transactional work. */
void VoltDBEngine::tick(int64_t timeInMillis, int64_t lastCommittedTxnId) {
    m_executorContext->setupForTick(lastCommittedTxnId, timeInMillis);

    // pass the tick to any tables that are interested
    typedef std::pair<int32_t, voltdb::Table*> TablePair;
    BOOST_FOREACH (TablePair table, m_tables) {
        table.second->flushOldTuples(timeInMillis);
    }
}

/** For now, bring the ELT system to a steady state with no buffers with content */
void VoltDBEngine::quiesce(int64_t lastCommittedTxnId) {
    m_executorContext->setupForQuiesce(lastCommittedTxnId);
    typedef std::pair<int32_t, voltdb::Table*> TablePair;
    BOOST_FOREACH (TablePair table, m_tables) {
        table.second->flushOldTuples(-1L);
    }
}

voltdb::StatsAgent& VoltDBEngine::getStatsManager() {
    return m_statsManager;
}

/**
 * Retrieve a set of statistics and place them into the result buffer as a set of VoltTables.
 * @param selector StatisticsSelectorType indicating what set of statistics should be retrieved
 * @param locators Integer identifiers specifying what subset of possible statistical sources should be polled. Probably a CatalogId
 *                 Can be NULL in which case all possible sources for the selector should be included.
 * @param numLocators Size of locators array.
 * @param interval Whether to return counters since the beginning or since the last time this was called
 * @param Timestamp to embed in each row
 *  @return Number of result tables, 0 on no results, -1 on failure.
 */
int VoltDBEngine::getStats(
        int selector,
        int locators[],
        int numLocators,
        bool interval,
        int64_t now) {
    voltdb::Table *resultTable = NULL;
    std::vector<voltdb::CatalogId> locatorIds;

    for (int ii = 0; ii < numLocators; ii++) {
        voltdb::CatalogId locator = static_cast<voltdb::CatalogId>(locators[ii]);
        locatorIds.push_back(locator);
    }
    std::size_t lengthPosition = m_resultOutput.reserveBytes(sizeof(int32_t));
    switch (selector) {
    case STATISTICS_SELECTOR_TYPE_TABLE: {
        for (int ii = 0; ii < numLocators; ii++) {
            voltdb::CatalogId locator = static_cast<voltdb::CatalogId>(locators[ii]);
            if (m_tables[locator] == NULL) {
                VOLT_ERROR("getStats() called with selector %d, and an invalid locator"
                        " %d that does not correspond to a table", selector, locator);
                return -1;
            }
        }
        resultTable = m_statsManager.getStats(
                (voltdb::StatisticsSelectorType)selector,
                locatorIds,
                interval,
                now);
        break;
    }
    default:
        VOLT_ERROR("getStats() called with an unrecognized selector %d", selector );
        return -1;
    }
    if (resultTable != NULL) {
        resultTable->serializeTo(m_resultOutput);
        m_resultOutput.writeIntAt( lengthPosition, static_cast<int32_t>(m_resultOutput.size() - sizeof(int32_t)));
        return 1;
    } else {
        return 0;
    }
}

/*
 * Exists to transition pre-existing unit test cases.
 */
ExecutorContext * VoltDBEngine::getExecutorContext() {
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum());
    return m_executorContext;
}

int64_t VoltDBEngine::uniqueIdForFragment(catalog::PlanFragment *frag) {
    int64_t retval = 0;
    catalog::CatalogType *parent = frag->parent();
    retval = static_cast<int64_t>(parent->parent()->relativeIndex()) << 32;
    retval += static_cast<int64_t>(parent->relativeIndex()) << 16;
    retval += static_cast<int64_t>(frag->relativeIndex());

    return retval;
}

/**
 * Activate copy on write mode for the specified table
 */
bool VoltDBEngine::activateCopyOnWrite(const CatalogId tableId) {
    PersistentTable *table = dynamic_cast<PersistentTable*>(m_tables[tableId]);
    assert(table != NULL);
    if (table == NULL) {
        return false;
    }

    if (table->activateCopyOnWrite(&m_tupleSerializer, m_partitionId)) {
        return false;
    }

    return true;
}

/**
 * Serialize more tuples from the specified table that is in COW mode.
 * Returns the number of bytes worth of tuple data serialized or 0 if there are no more.
 * Returns -1 if the table is no in COW mode. The table continues to be in COW (although no copies are made)
 * after all tuples have been serialize until the last call to cowSerializeMore which returns 0 (and deletes
 * the COW context). Further calls will return -1
 */
int VoltDBEngine::cowSerializeMore(ReferenceSerializeOutput *out, const CatalogId tableId) {
    PersistentTable *table = dynamic_cast<PersistentTable*>(m_tables[tableId]);
    assert(table != NULL);
    if (table == NULL) {
        return -1;
    }
    table->serializeMore(out);

    return static_cast<int>(out->position());
}

}
