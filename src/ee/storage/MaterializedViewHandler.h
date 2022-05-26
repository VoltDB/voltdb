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

#ifndef MATERIALIZEDVIEWHANDLER_H_
#define MATERIALIZEDVIEWHANDLER_H_

#include <utility>
#include <vector>
#include <map>

#include "catalog/materializedviewhandlerinfo.h"
#include "common/tabletuple.h"
#include "execution/ExecutorVector.h"
#include "storage/persistenttable.h"

namespace voltdb {

class VoltDBEngine;

// This class is used for enabling delta table mode on a view source table.
// It is a friend class of persistent table.
class ScopedDeltaTableContext {
public:
    ScopedDeltaTableContext(PersistentTable *table)
        : m_table(table) {
        vassert(m_table->m_deltaTable);
        vassert(! m_table->m_deltaTableActive);
        m_table->m_deltaTableActive = true;
    }

    ~ScopedDeltaTableContext() {
        m_table->m_deltaTableActive = false;
    }
private:
    PersistentTable *m_table;
};

class ReplicatedMaterializedViewHandler;

// Handle materialized view related events, particularly for views defined on join queries.
class MaterializedViewHandler {
public:
    // Create a MaterializedViewHandler based on the catalog info and install it to the view table.
    MaterializedViewHandler(PersistentTable* targetTable,
                            catalog::MaterializedViewHandlerInfo* mvHandlerInfo,
                            int32_t groupByColumnCount,
                            VoltDBEngine* engine);

    virtual ~MaterializedViewHandler();

    virtual void dropSourceTable(PersistentTable *sourceTable);

    // This is called to catch up with the existing data in the source tables.
    // It is useful when the view is created after the some data was inserted into the
    // source table(s).
    //
    // This must be done outside of the constructor, since the
    // catching up executes a plan fragment which may throw an
    // exception.
    virtual void catchUpWithExistingData(bool fallible);

    PersistentTable *destTable() const {
        return m_destTable;
    }
    /* A view handler becomes dirty (and needs to be recreated) when:
     * 1. One of the source table is re-created. This may result from:
     *    a. The source table was empty so it was deleted and re-created.
     *    b. The source table was truncated.
     *    c. The source table was replicated and a partition table statement was executed.
     * 2. Indices are changed on the source table (add/remove).
     */
    virtual bool isDirty() { return m_dirty; }
    virtual void pollute() { m_dirty = true; }
    virtual void setEnabled(bool enabled);
    virtual bool isEnabled() { return m_enabled; }
    virtual bool snapshotable() { return m_supportSnapshot; }

    // handleTupleInsert and handleTupleDelete are event handlers.
    // They are called when a source table has data being inserted / deleted.
    // The update operation is considered as a sequence of delete and insert operation.
    // When insertion and deletion happens on the source table, the affected
    // tuple will be inserted into a delta table affiliated with the source table.
    // The handler will move the source table into delta mode and execute the view definition query.
    virtual void handleTupleInsert(PersistentTable *sourceTable, bool fallible);
    virtual void handleTupleDelete(PersistentTable *sourceTable, bool fallible);

private:
    std::map<PersistentTable*, int32_t> m_sourceTables;
    // Make pointer immutable so it is safe for ReplicatedMaterializedViewHandler to use it's local copy
    PersistentTable * const m_destTable;
    // This is the index automatically created on view creation.
    TableIndex *m_index;
    // Vector of query plans (executors) for every min/max column.
    std::vector<boost::shared_ptr<ExecutorVector>> m_minMaxExecutorVectors;
    // The executor vector for the view definition query.
    boost::shared_ptr<ExecutorVector> m_createQueryExecutorVector;
    const int32_t m_groupByColumnCount;
    // Store the index of last COUNT(*) for optimization
    int m_countStarColumnIndex;
    int m_aggColumnCount;
    std::vector<ExpressionType> m_aggTypes;
    bool m_dirty;
    // Indicates whether the view can be included in a snapshot.
    // If a view is partitioned but there is not an explicit partition column,
    // then it cannot be included in a snapshot.
    bool m_supportSnapshot;
    // Indicates whether the view is enabled.
    bool m_enabled;
    // Both the existingTuple and the updatedTuple have the same schema of the view table.
    // The difference is the updatedTuple has its own storage, it's a standalone tuple.
    // existingTuple is used to search in the view table for the row with designated group-by columns.
    // updatedTuple is used to hold the updated values temporarily before it's inserted into the table.
    TableTuple m_existingTuple;
    TableTuple m_updatedTuple;
    StandAloneTupleStorage m_updatedTupleStorage;
    // vector of target table indexes to update.
    // Ideally, these should be a subset of the target table indexes that depend on the count and/or
    // aggregated columns, but there might be some other mostly harmless ones in there that are based
    // solely on the immutable primary key (GROUP BY columns).
    std::vector<TableIndex*> m_updatableIndexList;
    std::unique_ptr<ReplicatedMaterializedViewHandler> m_replicatedWrapper;

    // We maintain the source table list here to register / de-register the view handler on the source tables.
    void addSourceTable(bool viewHandlerPartitioned, PersistentTable *sourceTable,
            int32_t relativeTableIndex, VoltDBEngine* engine);
    void dropSourceTable(bool viewHandlerPartitioned, std::map<PersistentTable*, int32_t>::iterator);
    // Install the view handler to source / dest table(s).
    void install(catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                 VoltDBEngine *engine);
    void setUpAggregateInfo(catalog::MaterializedViewHandlerInfo *mvHandlerInfo);
    void setUpCreateQuery(catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                          VoltDBEngine *engine);
    void setUpMinMaxQueries(catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                            VoltDBEngine *engine);
    // Set up the m_existingTuple and the m_updatedTuple.
    void setUpBackedTuples();

    // Find in the view table (m_destTable) for the row that has the same group-by keys as
    // the deltaTuple.
    // The function will return a boolean value indicating if a matching row is found.
    // If a matching row is found, we can access its content from m_existingTuple.
    bool findExistingTuple(const TableTuple &deltaTuple);
    // Merge the deltaTuple with the existingTuple on an insert / delete context and
    // the resultant tuple is stored in m_updatedTuple.
    void mergeTupleForInsert(const TableTuple &deltaTuple);
    void mergeTupleForDelete(const TableTuple &deltaTuple);
    // Find a fallback min/max value for the designated column.
    // Please note that the minMaxColumnIndex is used to locate the correct query plan to execute.
    NValue fallbackMinMaxColumn(int columnIndex, int minMaxColumnIndex);
};

class ReplicatedMaterializedViewHandler : public MaterializedViewHandler {
public:
    // Create a MaterializedViewHandler based on the catalog info and install it to the view table.
    ReplicatedMaterializedViewHandler(PersistentTable* destTable, MaterializedViewHandler* partitionedHandler, int32_t partitionId);

    virtual void dropSourceTable(PersistentTable *sourceTable) {
        m_partitionedHandler->dropSourceTable(sourceTable);
    }

    // This is called to catch up with the existing data in the source tables.
    // It is useful when the view is created after the some data was inserted into the
    // source table(s).
    //
    // This must be done outside of the constructor, since the
    // catching up executes a plan fragment which may throw an
    // exception.
    virtual void catchUpWithExistingData(bool fallible) {
        m_partitionedHandler->catchUpWithExistingData(fallible);
    }

    /* A view handler becomes dirty (and needs to be recreated) when:
     * 1. One of the source table is re-created. This may result from:
     *    a. The source table was empty so it was deleted and re-created.
     *    b. The source table was truncated.
     *    c. The source table was replicated and a partition table statement was executed.
     * 2. Indices are changed on the source table (add/remove).
     */
    virtual bool isDirty() { return m_partitionedHandler->isDirty(); }
    virtual void pollute() { m_partitionedHandler->pollute(); }
    virtual void setEnabled(bool enabled) { m_partitionedHandler->setEnabled(enabled); }
    virtual bool isEnabled() { return m_partitionedHandler->isEnabled(); }
    virtual bool snapshotable() { return m_partitionedHandler->snapshotable(); }

    virtual void handleTupleInsert(PersistentTable *sourceTable, bool fallible);
    virtual void handleTupleDelete(PersistentTable *sourceTable, bool fallible);

private:
    MaterializedViewHandler* m_partitionedHandler;
    int32_t m_handlerPartitionId;
};

} // namespace voltdb
#endif // MATERIALIZEDVIEWHANDLER_H_
