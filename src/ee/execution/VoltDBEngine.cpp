/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#include "boost/shared_array.hpp"
#include "boost/scoped_array.hpp"
#include "boost/foreach.hpp"
#include "boost/scoped_ptr.hpp"
#include "boost/shared_ptr.hpp"
#include "VoltDBEngine.h"
#include "common/common.h"
#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/valuevector.h"
#include "common/TheHashinator.h"
#include "common/tabletuple.h"
#include "common/executorcontext.hpp"
#include "common/FatalException.hpp"
#include "common/RecoveryProtoMessage.h"
#include "common/TupleOutputStreamProcessor.h"
#include "common/LegacyHashinator.h"
#include "common/ElasticHashinator.h"
#include "catalog/catalogmap.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
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
#include "logging/LogManager.h"
#include "plannodes/abstractplannode.h"
#include "plannodes/abstractscannode.h"
#include "plannodes/plannodeutil.h"
#include "plannodes/plannodefragment.h"
#include "executors/executorutil.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "indexes/tableindex.h"
#include "storage/constraintutil.h"
#include "storage/persistenttable.h"
#include "storage/streamedtable.h"
#include "storage/MaterializedViewMetadata.h"
#include "storage/StreamBlock.h"
#include "storage/TableCatalogDelegate.hpp"
#include "org_voltdb_jni_ExecutionEngine.h" // to use static values
#include "stats/StatsAgent.h"
#include "voltdbipc.h"
#include "common/FailureInjection.h"

#include <iostream>
#include <stdio.h>
#include <fstream>
#include <errno.h>
#include <sstream>
#include <unistd.h>
#include <locale>
#ifdef LINUX
#include <malloc.h>
#endif // LINUX

using namespace std;
namespace voltdb {

const int64_t AD_HOC_FRAG_ID = -1;

VoltDBEngine::VoltDBEngine(Topend *topend, LogProxy *logProxy)
    : m_currentUndoQuantum(NULL),
      m_hashinator(NULL),
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
    // init the number of planfragments executed
    m_pfCount = 0;

    // require a site id, at least, to inititalize.
    m_executorContext = NULL;

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
}

bool
VoltDBEngine::initialize(int32_t clusterIndex,
                         int64_t siteId,
                         int32_t partitionId,
                         int32_t hostId,
                         string hostname,
                         int64_t tempTableMemoryLimit,
                         HashinatorType hashinatorType,
                         char *hashinatorConfig)
{
    // Be explicit about running in the standard C locale for now.
    locale::global(locale("C"));
    setenv("TZ", "UTC", 0); // set timezone as "UTC" in EE level
    m_clusterIndex = clusterIndex;
    m_siteId = siteId;
    m_partitionId = partitionId;
    m_tempTableMemoryLimit = tempTableMemoryLimit;

    // Instantiate our catalog - it will be populated later on by load()
    m_catalog = boost::shared_ptr<catalog::Catalog>(new catalog::Catalog());

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

    // required for catalog loading.
    m_executorContext = new ExecutorContext(siteId,
                                            m_partitionId,
                                            m_currentUndoQuantum,
                                            getTopend(),
                                            &m_stringPool,
                                            m_isELEnabled,
                                            hostname,
                                            hostId);

    switch (hashinatorType) {
    case HASHINATOR_LEGACY:
        m_hashinator.reset(LegacyHashinator::newInstance(hashinatorConfig));
        break;
    case HASHINATOR_ELASTIC:
        m_hashinator.reset(ElasticHashinator::newInstance(hashinatorConfig));
        break;
    default:
        throwFatalException("Unknown hashinator type %d", hashinatorType);
        break;
    }

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
    m_plans.clear();

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
    typedef pair<int64_t, Table*> TIDPair;

    BOOST_FOREACH (LabeledCDPair cdPair, m_catalogDelegates) {
        delete cdPair.second;
    }

    BOOST_FOREACH (TIDPair tidPair, m_snapshottingTables) {
        tidPair.second->decrementRefcount();
    }

    delete m_topend;
    delete m_executorContext;
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

Table* VoltDBEngine::getTable(string name) const
{
    // Caller responsible for checking null return value.
    return findInMapOrNull(name, m_tablesByName);
}

bool VoltDBEngine::serializeTable(int32_t tableId, SerializeOutput* out) const {
    // Just look in our list of tables
    Table* table = getTable(tableId);
    if (table) {
        table->serializeTo(*out);
        return true;
    } else {
        throwFatalException( "Unable to find table for TableId '%d'", (int) tableId);
    }
}

// ------------------------------------------------------------------
// EXECUTION FUNCTIONS
// ------------------------------------------------------------------
int VoltDBEngine::executeQuery(int64_t planfragmentId,
                               int32_t outputDependencyId,
                               int32_t inputDependencyId,
                               const NValueArray &params,
                               int64_t spHandle, int64_t lastCommittedSpHandle,
                               int64_t uniqueId,
                               bool first, bool last)
{
    assert(planfragmentId != 0);

    Table *cleanUpTable = NULL;
    m_currentOutputDepId = outputDependencyId;
    m_currentInputDepId = inputDependencyId;

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

    // set this to zero for dml operations
    m_tuplesModified = 0;

    /*
     * Reserve space in the result output buffer for the number of
     * result dependencies generated by this particular plan fragment.
     * Necessary for a plan fragment because the
     * number of produced depenencies may not be known in advance.
     */
    m_numResultDependencies = 0;
    size_t numResultDependenciesCountOffset = m_resultOutput.reserveBytes(4);

    // configure the execution context.
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(),
                                             spHandle,
                                             lastCommittedSpHandle,
                                             uniqueId);

    // count the number of plan fragments executed
    ++m_pfCount;

    // execution lists for planfragments are cached by planfragment id
    ExecutorVector *execsForFrag = NULL;
    try {
        execsForFrag = getExecutorVectorForFragmentId(planfragmentId);
    }
    catch (const SerializableEEException &e) {
        resetReusedResultOutputBuffer();
        e.serialize(getExceptionOutputSerializer());

        // set these back to -1 for error handling
        m_currentOutputDepId = -1;
        m_currentInputDepId = -1;
        return ENGINE_ERRORCODE_ERROR;
    }
    assert(execsForFrag);

    // Walk through the queue and execute each plannode.  The query
    // planner guarantees that for a given plannode, all of its
    // children are positioned before it in this list, therefore
    // dependency tracking is not needed here.
    size_t ttl = execsForFrag->list.size();
    for (int ctr = 0; ctr < ttl; ++ctr) {
        AbstractExecutor *executor = execsForFrag->list[ctr];
        assert (executor);

        if (executor->needsPostExecuteClear())
            cleanUpTable =
                dynamic_cast<Table*>(executor->getPlanNode()->getOutputTable());

        try {
            // Now call the execute method to actually perform whatever action
            // it is that the node is supposed to do...
            if (!executor->execute(params)) {
                VOLT_TRACE("The Executor's execution at position '%d'"
                           " failed for PlanFragment '%jd'",
                           ctr, (intmax_t)planfragmentId);
                if (cleanUpTable != NULL)
                    cleanUpTable->deleteAllTuples(false);
                // set these back to -1 for error handling
                m_currentOutputDepId = -1;
                m_currentInputDepId = -1;
                return ENGINE_ERRORCODE_ERROR;
            }
        } catch (const SerializableEEException &e) {
            VOLT_TRACE("The Executor's execution at position '%d'"
                       " failed for PlanFragment '%jd'",
                       ctr, (intmax_t)planfragmentId);
            if (cleanUpTable != NULL)
                cleanUpTable->deleteAllTuples(false);
            resetReusedResultOutputBuffer();
            e.serialize(getExceptionOutputSerializer());

            // set these back to -1 for error handling
            m_currentOutputDepId = -1;
            m_currentInputDepId = -1;
            return ENGINE_ERRORCODE_ERROR;
        }
    }
    if (cleanUpTable != NULL)
        cleanUpTable->deleteAllTuples(false);

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

    return true;
}

bool VoltDBEngine::loadCatalog(const int64_t timestamp, const string &catalogPayload) {
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

     // initialize the list of partition ids
    bool success = initCluster();
    if (success == false) {
        VOLT_ERROR("Unable to load partition list for cluster");
        return false;
    }

    // Tables care about EL state.
    if (m_database->connectors().size() > 0 &&
        m_database->connectors().get("0")->enabled())
    {
        VOLT_DEBUG("EL enabled.");
        m_executorContext->m_exportEnabled = true;
        m_isELEnabled = true;
    }

    // load up all the tables, adding all tables
    if (processCatalogAdditions(true, timestamp) == false) {
        return false;
    }

    rebuildTableCollections();

    // load up all the materialized views
    initMaterializedViews(true);

    VOLT_DEBUG("Loaded catalog...");
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
VoltDBEngine::processCatalogDeletes(int64_t timestamp )
{
    vector<string> deletions;
    m_catalog->getDeletedPaths(deletions);

    BOOST_FOREACH(string path, deletions) {
        VOLT_TRACE("delete path:");

        map<string, CatalogDelegate*>::iterator pos = m_catalogDelegates.find(path);
        if (pos == m_catalogDelegates.end()) {
           continue;
        }
        CatalogDelegate *delegate = pos->second;
        TableCatalogDelegate *tcd = dynamic_cast<TableCatalogDelegate*>(delegate);
        /*
         * Instruct the table to flush all export data
         * Then tell it about the new export generation/catalog txnid
         * which will cause it to notify the topend export data source
         * that no more data is coming for the previous generation
         */
        if (tcd) {
            Table *table = tcd->getTable();
            m_delegatesByName.erase(table->name());
            StreamedTable *streamedtable = dynamic_cast<StreamedTable*>(table);
            if (streamedtable) {
                m_exportingTables.erase(tcd->signature());
                streamedtable->setSignatureAndGeneration( tcd->signature(), timestamp);
            }
        }
        delegate->deleteCommand();
        delete delegate;
        m_catalogDelegates.erase(pos);
    }
}

bool
VoltDBEngine::hasSameSchema(catalog::Table *t1, voltdb::Table *t2) {
    // covers column count
    if (t1->columns().size() != t2->columnCount()) {
        return false;
    }

    // make sure each column has same metadata
    map<string, catalog::Column*>::const_iterator outerIter;
    for (outerIter = t1->columns().begin();
         outerIter != t1->columns().end();
         outerIter++)
    {
        int index = outerIter->second->index();
        int size = outerIter->second->size();
        int32_t type = outerIter->second->type();
        std::string name = outerIter->second->name();
        bool nullable = outerIter->second->nullable();

        if (t2->columnName(index).compare(name)) {
            return false;
        }

        if (t2->schema()->columnAllowNull(index) != nullable) {
            return false;
        }

        if (t2->schema()->columnType(index) != type) {
            return false;
        }

        // check the size of types where size matters
        if ((type == VALUE_TYPE_VARCHAR) || (type == VALUE_TYPE_VARBINARY)) {
            if (t2->schema()->columnLength(index) != size) {
                return false;
            }
        }
    }

    return true;
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
VoltDBEngine::processCatalogAdditions(bool addAll, int64_t timestamp)
{
    // iterate over all of the tables in the new catalog
    map<string, catalog::Table*>::const_iterator catTableIter;
    for (catTableIter = m_database->tables().begin();
         catTableIter != m_database->tables().end();
         catTableIter++)
    {
        // get the catalog's table object
        catalog::Table *catalogTable = catTableIter->second;
        if (addAll || catalogTable->wasAdded()) {
            VOLT_TRACE("add a completely new table...");

            //////////////////////////////////////////
            // add a completely new table
            //////////////////////////////////////////

            TableCatalogDelegate *tcd = new TableCatalogDelegate(catalogTable->relativeIndex(),
                                                                 catalogTable->path(),
                                                                 catalogTable->signature());

            // use the delegate to init the table and create indexes n' stuff
            if (tcd->init(*m_database, *catalogTable) != 0) {
                VOLT_ERROR("Failed to initialize table '%s' from catalog",
                           catTableIter->second->name().c_str());
                return false;
            }
            m_catalogDelegates[tcd->path()] = tcd;
            m_delegatesByName[tcd->getTable()->name()] = tcd;

            // set export info on the new table
            if (tcd->exportEnabled()) {
                tcd->getTable()->setSignatureAndGeneration(catalogTable->signature(), timestamp);
                m_exportingTables[catalogTable->signature()] = tcd->getTable();
            }
        }
        else {

            //////////////////////////////////////////////
            // update the export info for existing tables
            //
            // add/modify/remove indexes that have changed
            //  in the catalog
            //////////////////////////////////////////////

            // get the delegate and bail if it's not here
            // - JHH: I'm not sure why not finding a delegate is safe to ignore
            CatalogDelegate* delegate = findInMapOrNull(catalogTable->path(), m_catalogDelegates);
            TableCatalogDelegate *tcd = dynamic_cast<TableCatalogDelegate*>(delegate);
            if (!tcd) {
                continue;
            }

            Table *table = tcd->getTable();

            PersistentTable *persistenttable = dynamic_cast<PersistentTable*>(table);
            /*
             * Instruct the table that was not added but is being retained to flush
             * Then tell it about the new export generation/catalog txnid
             * which will cause it to notify the topend export data source
             * that no more data is coming for the previous generation
             */
            if ( ! persistenttable) {
                StreamedTable *streamedtable = dynamic_cast<StreamedTable*>(table);
                assert(streamedtable);
                streamedtable->setSignatureAndGeneration(catalogTable->signature(), timestamp);
                // note, this is the end of the line for export tables for now,
                // don't allow them to change schema yet
                continue;
            }

            //////////////////////////////////////////
            // if the table schema has changed, build a new
            // table and migrate tuples over to it, repopulating
            // indexes as we go
            //////////////////////////////////////////

            if (!hasSameSchema(catalogTable, persistenttable)) {
                char msg[512];
                snprintf(msg, sizeof(msg), "Table %s has changed schema and will be rebuilt.",
                         catalogTable->name().c_str());
                LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_INFO, msg);

                tcd->processSchemaChanges(*m_database, *catalogTable, m_delegatesByName);

                snprintf(msg, sizeof(msg), "Table %s was successfully rebuilt with new schema.",
                         catalogTable->name().c_str());
                LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_INFO, msg);

                // don't continue on to modify/add/remove indexes, because the
                // call above should rebuild them all anyway
                continue;
            }

            //////////////////////////////////////////
            // find all of the indexes to add
            //////////////////////////////////////////

            const vector<TableIndex*> currentIndexes = persistenttable->allIndexes();

            // iterate over indexes for this table in the catalog
            map<string, catalog::Index*>::const_iterator indexIter;
            for (indexIter = catalogTable->indexes().begin();
                 indexIter != catalogTable->indexes().end();
                 indexIter++)
            {
                std::string indexName = indexIter->first;
                std::string catalogIndexId = TableCatalogDelegate::getIndexIdString(*indexIter->second);

                // Look for an index on the table to match the catalog index
                bool found = false;
                for (int i = 0; i < currentIndexes.size(); i++) {
                    std::string currentIndexId = currentIndexes[i]->getId();
                    if (catalogIndexId == currentIndexId) {
                        // rename the index if needed (or even if not)
                        currentIndexes[i]->rename(indexName);
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    VOLT_TRACE("create and add the index...");
                    // create and add the index
                    TableIndexScheme scheme;
                    bool success = TableCatalogDelegate::getIndexScheme(*catalogTable,
                                                                        *indexIter->second,
                                                                        persistenttable->schema(),
                                                                        &scheme);
                    if (!success) {
                        VOLT_ERROR("Failed to initialize index '%s' from catalog",
                                   indexIter->second->name().c_str());
                        return false;
                    }

                    TableIndex *index = TableIndexFactory::getInstance(scheme);
                    assert(index);

                    // all of the data should be added here
                    persistenttable->addIndex(index);

                    // add the index to the stats source
                    index->getIndexStats()->configure(index->getName() + " stats",
                                                      persistenttable->name(),
                                                      indexIter->second->relativeIndex());
                }
            }

            //////////////////////////////////////////
            // now find all of the indexes to remove
            //////////////////////////////////////////

            bool found = false;
            // iterate through all of the existing indexes
            for (int i = 0; i < currentIndexes.size(); i++) {
                std::string currentIndexId = currentIndexes[i]->getId();

                // iterate through all of the catalog indexes,
                //  looking for a match.
                map<string, catalog::Index*>::const_iterator indexIter;
                for (indexIter = catalogTable->indexes().begin();
                     indexIter != catalogTable->indexes().end();
                     indexIter++)
                {
                    std::string catalogIndexId = TableCatalogDelegate::getIndexIdString(*indexIter->second);
                    if (catalogIndexId == currentIndexId) {
                        found = true;
                        break;
                    }
                }

                // if the table has an index that the catalog doesn't,
                // then remove the index
                if (!found) {
                    persistenttable->removeIndex(currentIndexes[i]);
                }
            }

            ///////////////////////////////////////////////////
            // now find all of the materialized views to remove
            ///////////////////////////////////////////////////

            vector<catalog::MaterializedViewInfo*> survivingInfos;
            vector<MaterializedViewMetadata*> survivingViews;
            vector<MaterializedViewMetadata*> obsoleteViews;

            const catalog::CatalogMap<catalog::MaterializedViewInfo> & views = catalogTable->views();
            persistenttable->segregateMaterializedViews(views.begin(), views.end(),
                                                        survivingInfos, survivingViews,
                                                        obsoleteViews);

            // This process temporarily duplicates the materialized view definitions and their
            // target table reference counts for all the right materialized view tables,
            // leaving the others to go away with the existingTable.
            // Since this is happening "mid-stream" in the redefinition of all of the source and target tables,
            // there needs to be a way to handle cases where the target table HAS been redefined already and
            // cases where it HAS NOT YET been redefined (and cases where it just survives intact).
            // At this point, the materialized view makes a best effort to use the
            // current/latest version of the table -- particularly, because it will have made off with the
            // "old" version's primary key index, which is used in the MaterializedViewMetadata constructor.
            // Once ALL tables have been added/(re)defined, any materialized view definitions that still use
            // an obsolete target table needs to be brought forward to reference the replacement table.
            // See initMaterializedViews

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
                DEBUG_STREAM_HERE("Adding new mat view " << targetTable->name() << "@" << targetTable <<
                                  " was @" << oldTargetTable <<
                                  " on " << persistenttable->name() << "@" << persistenttable);
                // This is not a leak -- the view metadata is self-installing into the new table.
                // Also, it guards its targetTable from accidental deletion with a refcount bump.
                new MaterializedViewMetadata(persistenttable, targetTable, currInfo);
                obsoleteViews.push_back(survivingViews[ii]);
            }

            BOOST_FOREACH(MaterializedViewMetadata * toDrop, obsoleteViews) {
                persistenttable->dropMaterializedView(toDrop);
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
VoltDBEngine::updateCatalog(const int64_t timestamp, const string &catalogPayload)
{
    // clean up execution plans when the tables underneath might change
    m_plans.clear();

    assert(m_catalog != NULL); // the engine must be initialized

    VOLT_DEBUG("Updating catalog...");

    // apply the diff commands to the existing catalog
    // throws SerializeEEExceptions on error.
    m_catalog->execute(catalogPayload);

    if (updateCatalogDatabaseReference() == false) {
        VOLT_ERROR("Error re-caching catalog references.");
        return false;
    }

    processCatalogDeletes(timestamp);

    if (processCatalogAdditions(false, timestamp) == false) {
        VOLT_ERROR("Error processing catalog additions.");
        return false;
    }

    rebuildTableCollections();

    initMaterializedViews(false);

    m_catalog->purgeDeletions();
    VOLT_DEBUG("Updated catalog...");
    return true;
}

bool
VoltDBEngine::loadTable(int32_t tableId,
                        ReferenceSerializeInput &serializeIn,
                        int64_t spHandle, int64_t lastCommittedSpHandle,
                        bool returnUniqueViolations)
{
    //Not going to thread the unique id through.
    //The spHandle and lastCommittedSpHandle aren't really used in load table
    //since their only purpose as of writing this (1/2013) they are only used
    //for export data and we don't technically support loading into an export table
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(),
                                             spHandle,
                                             -1,
                                             lastCommittedSpHandle);

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
        table->loadTuplesFrom(serializeIn, NULL, returnUniqueViolations ? getResultOutputSerializer() : NULL);
    } catch (const SerializableEEException &e) {
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

    // need to re-map all the table ids / indexes
    getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_TABLE);
    getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_INDEX);

    // walk the table delegates and update local table collections
    BOOST_FOREACH (LabeledCDPair cdPair, m_catalogDelegates) {
        TableCatalogDelegate *tcd = dynamic_cast<TableCatalogDelegate*>(cdPair.second);
        if (tcd) {
            catalog::Table *catTable = m_database->tables().get(tcd->getTable()->name());
            m_tables[catTable->relativeIndex()] = tcd->getTable();
            m_tablesByName[tcd->getTable()->name()] = tcd->getTable();

            getStatsManager().registerStatsSource(STATISTICS_SELECTOR_TYPE_TABLE,
                                                  catTable->relativeIndex(),
                                                  tcd->getTable()->getTableStats());

            // add all of the indexes to the stats source
            const std::vector<TableIndex*>& tindexes = tcd->getTable()->allIndexes();
            for (int i = 0; i < tindexes.size(); i++) {
                TableIndex *index = tindexes[i];
                getStatsManager().registerStatsSource(STATISTICS_SELECTOR_TYPE_INDEX,
                                                      catTable->relativeIndex(),
                                                      index->getIndexStats());
            }
        }
    }
}

VoltDBEngine::ExecutorVector *VoltDBEngine::getExecutorVectorForFragmentId(const int64_t fragId) {
    typedef PlanSet::nth_index<1>::type plansById;
    plansById::iterator iter = m_plans.get<1>().find(fragId);

    // found it, move it to the front
    if (iter != m_plans.get<1>().end()) {
        // move it to the front of the list
        PlanSet::iterator iter2 = m_plans.project<0>(iter);
        m_plans.get<0>().relocate(m_plans.begin(), iter2);
        VoltDBEngine::ExecutorVector *retval = (*iter).get();
        assert(retval);
        return retval;
    }
    else {
        std::string plan = m_topend->planForFragmentId(fragId);

        if (plan.length() == 0) {
            char msg[1024];
            snprintf(msg, 1024, "Fetched empty plan from frontend for PlanFragment '%jd'",
                     (intmax_t)fragId);
            VOLT_ERROR("%s", msg);
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, msg);
        }

        PlanNodeFragment *pnf = NULL;
        try {
            pnf = PlanNodeFragment::createFromCatalog(plan);
        }
        catch (SerializableEEException &seee) {
            throw;
        }
        catch (...) {
            char msg[1024 * 100];
            snprintf(msg, 1024 * 100, "Unable to initialize PlanNodeFragment for PlanFragment '%jd' with plan:\n%s",
                     (intmax_t)fragId, plan.c_str());
            VOLT_ERROR("%s", msg);
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, msg);
        }
        VOLT_TRACE("\n%s\n", pnf->debug().c_str());
        assert(pnf->getRootNode());

        if (!pnf->getRootNode()) {
            char msg[1024];
            snprintf(msg, 1024, "Deserialized PlanNodeFragment for PlanFragment '%jd' does not have a root PlanNode",
                     (intmax_t)fragId);
            VOLT_ERROR("%s", msg);
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, msg);
        }

        // ENG-1333 HACK.  If the plan node fragment has a delete node,
        // then turn off the governors
        int64_t frag_temptable_log_limit = (m_tempTableMemoryLimit * 3) / 4;
        int64_t frag_temptable_limit = m_tempTableMemoryLimit;
        if (pnf->hasDelete())
        {
            frag_temptable_log_limit = DEFAULT_TEMP_TABLE_MEMORY;
            frag_temptable_limit = -1;
        }

        boost::shared_ptr<ExecutorVector> ev(new ExecutorVector(fragId, frag_temptable_log_limit, frag_temptable_limit, pnf));

        // Initialize each node!
        for (int ctr = 0, cnt = (int)pnf->getExecuteList().size();
             ctr < cnt; ctr++) {
            if (!initPlanNode(fragId, pnf->getExecuteList()[ctr], &(ev->limits)))
            {
                char msg[1024 * 10];
                snprintf(msg, 1024 * 10, "Failed to initialize PlanNode '%s' at position '%d' for PlanFragment '%jd'",
                         pnf->getExecuteList()[ctr]->debug().c_str(), ctr, (intmax_t)fragId);
                VOLT_ERROR("%s", msg);
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, msg);
            }
        }

        // Initialize the vector of executors for this planfragment, used at runtime.
        for (int ctr = 0, cnt = (int)pnf->getExecuteList().size(); ctr < cnt; ctr++) {
            ev->list.push_back(pnf->getExecuteList()[ctr]->getExecutor());
        }

        // add the plan to the back
        m_plans.get<0>().push_back(ev);

        // remove a plan from the front if the cache is full
        if (m_plans.size() > PLAN_CACHE_SIZE) {
            PlanSet::iterator iter = m_plans.get<0>().begin();
            m_plans.erase(iter);
        }

        VoltDBEngine::ExecutorVector *retval = ev.get();
        assert(retval);
        return retval;
    }

    return NULL;
}

// -------------------------------------------------
// Initialization Functions
// -------------------------------------------------

bool VoltDBEngine::initPlanNode(const int64_t fragId,
                                AbstractPlanNode* node,
                                TempTableLimits* limits)
{
    assert(node);
    assert(node->getExecutor() == NULL);

    // Executor is created here. An executor is *devoted* to this plannode
    // so that it can cache anything for the plannode
    AbstractExecutor* executor = getNewExecutor(this, node);
    if (executor == NULL)
        return false;
    node->setExecutor(executor);

    // If this PlanNode has an internal PlanNode (e.g., AbstractScanPlanNode can
    // have internal Projections), then we need to make sure that we set that
    // internal node's executor as well
    if (node->getInlinePlanNodes().size() > 0) {
        map<PlanNodeType, AbstractPlanNode*>::iterator internal_it;
        for (internal_it = node->getInlinePlanNodes().begin();
             internal_it != node->getInlinePlanNodes().end(); internal_it++) {
            AbstractPlanNode* inline_node = internal_it->second;
            if (!initPlanNode(fragId, inline_node, limits))
            {
                VOLT_ERROR("Failed to initialize the internal PlanNode '%s' of"
                           " PlanNode '%s'", inline_node->debug().c_str(),
                           node->debug().c_str());
                return false;
            }
        }
    }

    // Now use the executor to initialize the plannode for execution later on
    if (!executor->init(this, limits))
    {
        VOLT_ERROR("The Executor failed to initialize PlanNode '%s' for"
                   " PlanFragment '%jd'", node->debug().c_str(),
                   (intmax_t)fragId);
        return false;
    }

    return true;
}

/*
 * Iterate catalog tables looking for tables that are materialized
 * view sources.  When found, construct a materialized view metadata
 * object that connects the source and destination tables, and assign
 * that object to the source table.
 *
 * Assumes all tables (sources and destinations) have been constructed.
 * @param addAll Pass true to add all views. Pass false to only add new views.
 */
void VoltDBEngine::initMaterializedViews(bool addAll) {
    map<string, catalog::Table*>::const_iterator tableIterator;
    const map<string, catalog::Table*>::const_iterator begin = m_database->tables().begin();
    const map<string, catalog::Table*>::const_iterator end = m_database->tables().end();
    // walk tables
    for (tableIterator = begin; tableIterator != end; tableIterator++) {
        catalog::Table *srcCatalogTable = tableIterator->second;
        PersistentTable *srcTable = dynamic_cast<PersistentTable*>(m_tables[srcCatalogTable->relativeIndex()]);
        // walk views
        const catalog::CatalogMap<catalog::MaterializedViewInfo> & views = srcCatalogTable->views();
        map<string, catalog::MaterializedViewInfo*>::const_iterator matviewIterator;
        map<string, catalog::MaterializedViewInfo*>::const_iterator begin = views.begin();
        map<string, catalog::MaterializedViewInfo*>::const_iterator end = views.end();
        for (matviewIterator = begin; matviewIterator != end; ++matviewIterator) {
            assert(srcTable);
            catalog::MaterializedViewInfo *catalogView = matviewIterator->second;
            const catalog::Table *destCatalogTable = catalogView->dest();
            PersistentTable *destTable = dynamic_cast<PersistentTable*>(m_tables[destCatalogTable->relativeIndex()]);
            // connect source and destination tables
            if (addAll || catalogView->wasAdded()) {
                DEBUG_STREAM_HERE("Adding new mat view " << destTable->name() <<
                                  " on " << srcTable->name());
                // This is not a leak -- the materialized view is self-installing into srcTable.
                new MaterializedViewMetadata(srcTable, destTable, catalogView);
            } else {
                // Ensure that the materialized view is using the latest version of the target table.
                srcTable->updateMaterializedViewTargetTable(destTable);
            }
        }
    }
}

bool VoltDBEngine::initCluster() {

    catalog::Cluster* catalogCluster =
      m_catalog->clusters().get("cluster");

    // deal with the epoch
    int64_t epoch = catalogCluster->localepoch() * (int64_t)1000;
    m_executorContext->setEpoch(epoch);

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
// MISC FUNCTIONS
// -------------------------------------------------

void VoltDBEngine::printReport() {
    cout << "==========" << endl;
    cout << "==========" << endl;
}

bool VoltDBEngine::isLocalSite(const NValue& value)
{
    int index = m_hashinator->hashinate(value);
    return index == m_partitionId;
}

/** Perform once per second, non-transactional work. */
void VoltDBEngine::tick(int64_t timeInMillis, int64_t lastCommittedSpHandle) {
    m_executorContext->setupForTick(lastCommittedSpHandle);
    typedef pair<string, Table*> TablePair;
    BOOST_FOREACH (TablePair table, m_exportingTables) {
        table.second->flushOldTuples(timeInMillis);
    }
}

/** For now, bring the Export system to a steady state with no buffers with content */
void VoltDBEngine::quiesce(int64_t lastCommittedSpHandle) {
    m_executorContext->setupForQuiesce(lastCommittedSpHandle);
    typedef pair<string, Table*> TablePair;
    BOOST_FOREACH (TablePair table, m_exportingTables) {
        table.second->flushOldTuples(-1L);
    }
}

string VoltDBEngine::debug(void) const {
    stringstream output(stringstream::in | stringstream::out);
    PlanSet::const_iterator iter;
    vector<AbstractExecutor*>::const_iterator executorIter;

    for (iter = m_plans.begin(); iter != m_plans.end(); iter++) {
        boost::shared_ptr<ExecutorVector> ev = *iter;

        output << "Fragment ID: " << ev->fragId << ", "
               << "Executor list size: " << ev->list.size() << ", "
               << "Temp table memory in bytes: "
               << ev->limits.getAllocated() << endl;

        for (executorIter = ev->list.begin();
             executorIter != ev->list.end();
             executorIter++) {
            output << (*executorIter)->getPlanNode()->debug(" ") << endl;
        }
    }

    return output.str();
}

StatsAgent& VoltDBEngine::getStatsManager() {
    return m_statsManager;
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
    vector<CatalogId> locatorIds;

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
    } catch (const SerializableEEException &e) {
        resetReusedResultOutputBuffer();
        e.serialize(getExceptionOutputSerializer());
        return -1;
    }

    if (resultTable != NULL) {
        resultTable->serializeTo(m_resultOutput);
        m_resultOutput.writeIntAt(lengthPosition,
                                  static_cast<int32_t>(m_resultOutput.size()
                                                       - sizeof(int32_t)));
        return 1;
    } else {
        return 0;
    }
}


void VoltDBEngine::setCurrentUndoQuantum(voltdb::UndoQuantum* undoQuantum)
{
    m_currentUndoQuantum = undoQuantum;
    m_executorContext->setupForPlanFragments(m_currentUndoQuantum);
}


/*
 * Exists to transition pre-existing unit test cases.
 */
ExecutorContext * VoltDBEngine::getExecutorContext() {
    m_executorContext->setupForPlanFragments(m_currentUndoQuantum);
    return m_executorContext;
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
        ReferenceSerializeInput &serializeIn) {
    Table* found = getTable(tableId);
    if (! found) {
        return false;
    }

    PersistentTable *table = dynamic_cast<PersistentTable*>(found);
    if (table == NULL) {
        assert(table != NULL);
        return false;
    }

    // Crank up the necessary persistent table streaming mechanism(s).
    if (table->activateStream(m_tupleSerializer, streamType, m_partitionId, tableId, serializeIn)) {
        return false;
    }

    // keep track of snapshotting tables. a table already in cow mode
    // can not be re-activated for cow mode.
    if (streamType == TABLE_STREAM_SNAPSHOT) {
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
 * Returns:
 *  0-n: remaining tuple count
 *  -1: streaming was completed by the previous call
 *  -2: error, e.g. when no longer in COW mode.
 * Note that -1 is only returned once after the previous call serialized all
 * remaining tuples. Further calls are considered errors and will return -2.
 */
int64_t VoltDBEngine::tableStreamSerializeMore(const CatalogId tableId,
                                               const TableStreamType streamType,
                                               ReferenceSerializeInput &serialize_in)
{
    int64_t remaining = -2;
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
            for (std::vector<int>::const_iterator ipos = positions.begin();
                 ipos != positions.end(); ++ipos) {
                results.writeInt(*ipos);
            }
        }
        VOLT_DEBUG("tableStreamSerializeMore: deserialized %d buffers, %ld remaining",
                   (int)positions.size(), remaining);
    }
    catch (SerializableEEException &e) {
        resetReusedResultOutputBuffer();
        e.serialize(getExceptionOutputSerializer());
        remaining = -2; // error
    }

    return remaining;
}

/**
 * Serialize tuples to output streams from a table in COW mode.
 * Overload that populates a position vector provided by the caller.
 * Returns:
 *  0-n: remaining tuple count
 *  -1: streaming was completed by the previous call
 *  -2: error, e.g. when no longer in COW mode.
 * Note that -1 is only returned once after the previous call serialized all
 * remaining tuples. Further calls are considered errors and will return -2.
 */
int64_t VoltDBEngine::tableStreamSerializeMore(
        const CatalogId tableId,
        const TableStreamType streamType,
        ReferenceSerializeInput &serializeIn,
        std::vector<int> &retPositions)
{
    // Deserialize the output buffer ptr/offset/length values into a COWStreamProcessor.
    int nBuffers = serializeIn.readInt();
    if (nBuffers <= 0) {
        throwFatalException(
                "Expected at least one output stream in tableStreamSerializeMore(), received %d",
                nBuffers);
    }
    TupleOutputStreamProcessor outputStreams(nBuffers);
    for (int iBuffer = 0; iBuffer < nBuffers; iBuffer++) {
        char *ptr = reinterpret_cast<char*>(serializeIn.readLong());
        int offset = serializeIn.readInt();
        int length = serializeIn.readInt();
        outputStreams.add(ptr + offset, length - offset);
    }
    retPositions.reserve(nBuffers);

    // Find the table based on what kind of stream we have.
    // If a completed table is polled, return remaining==-1. The
    // Java engine will always poll a fully serialized table one more
    // time (it doesn't see the hasMore return code).
    int64_t remaining = -1;
    PersistentTable *table = NULL;
    switch (streamType) {
        case TABLE_STREAM_SNAPSHOT: {
            // If a completed table is polled, return 0 bytes serialized. The
            // Java engine will always poll a fully serialized table one more
            // time (it doesn't see the hasMore return code).  Note that the
            // dynamic cast was already verified in activateCopyOnWrite.
            table = findInMapOrNull(tableId, m_snapshottingTables);
            break;
        }

        case TABLE_STREAM_RECOVERY: {
            Table* found = getTable(tableId);
            if (found) {
                table = dynamic_cast<PersistentTable*>(found);
            }
            break;
        }

        default:
            // Failure.
            return -2;
    }

    // Perform the streaming.
    if (table != NULL) {
        remaining = table->streamMore(outputStreams, retPositions);

        // Clear it from the snapshot table as appropriate.
        if (remaining <= 0 && streamType == TABLE_STREAM_SNAPSHOT) {
            m_snapshottingTables.erase(tableId);
            table->decrementRefcount();
        }
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
    table->processRecoveryMessage(message, NULL);
}

int64_t
VoltDBEngine::exportAction(bool syncAction, int64_t ackOffset, int64_t seqNo, std::string tableSignature)
{
    map<string, Table*>::iterator pos = m_exportingTables.find(tableSignature);

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

    map<string, Table*>::iterator pos = m_exportingTables.find(tableSignature);

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

void VoltDBEngine::updateHashinator(HashinatorType type, const char *config) {
    switch (type) {
    case HASHINATOR_LEGACY:
        m_hashinator.reset(LegacyHashinator::newInstance(config));
        break;
    case HASHINATOR_ELASTIC:
        m_hashinator.reset(ElasticHashinator::newInstance(config));
        break;
    default:
        throwFatalException("Unknown hashinator type %d", type);
        break;
    }
}

void VoltDBEngine::dispatchValidatePartitioningTask(const char *taskParams) {
    ReferenceSerializeInput taskInfo(taskParams, std::numeric_limits<std::size_t>::max());
    std::vector<CatalogId> tableIds;
    const int32_t numTables = taskInfo.readInt();
    for (int ii = 0; ii < numTables; ii++) {
        tableIds.push_back(static_cast<int32_t>(taskInfo.readLong()));
    }

    HashinatorType type = static_cast<HashinatorType>(taskInfo.readInt());
    const char *config = taskParams + (sizeof(int32_t) * 2) +  (sizeof(int64_t) * tableIds.size());
    boost::scoped_ptr<TheHashinator> hashinator;
    switch(type) {
        case HASHINATOR_LEGACY:
            hashinator.reset(LegacyHashinator::newInstance(config));
            break;
        case HASHINATOR_ELASTIC:
            hashinator.reset(ElasticHashinator::newInstance(config));
            break;
        default:
            throwFatalException("Unknown hashinator type %d", type);
            break;
    }

    std::vector<int64_t> mispartitionedRowCounts;

    BOOST_FOREACH( CatalogId tableId, tableIds) {
        std::map<CatalogId, Table*>::iterator table = m_tables.find(tableId);
        if (table == m_tables.end()) {
            throwFatalException("Unknown table id %d", tableId);
        } else {
            mispartitionedRowCounts.push_back(m_tables[tableId]->validatePartitioning(hashinator.get(), m_partitionId));
        }
    }

    ReferenceSerializeOutput *output = getResultOutputSerializer();
    output->writeInt(static_cast<int32_t>(sizeof(int64_t) * numTables));

    BOOST_FOREACH( int64_t mispartitionedRowCount, mispartitionedRowCounts) {
        output->writeLong(mispartitionedRowCount);
    }
}

void VoltDBEngine::executeTask(TaskType taskType, const char* taskParams) {
    switch (taskType) {
    case TASK_TYPE_VALIDATE_PARTITIONING:
        dispatchValidatePartitioningTask(taskParams);
        break;
    default:
        throwFatalException("Unknown task type %d", taskType);
    }
}


}
