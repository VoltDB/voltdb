/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include <vector>
#include <map>

#include "catalog/materializedviewhandlerinfo.h"
#include "common/tabletuple.h"
#include "execution/ExecutorVector.h"
#include "persistenttable.h"

namespace voltdb {

class VoltDBEngine;

class ScopedDeltaTableContext {
public:
    ScopedDeltaTableContext(PersistentTable *table)
        : m_table(table) {
        assert(m_table->m_deltaTable);
        assert( ! m_table->m_deltaTableActive);
        m_table->m_deltaTableActive = true;
    }

    ~ScopedDeltaTableContext() {
        m_table->m_deltaTableActive = false;
    }
private:
    PersistentTable *m_table;
};

class MaterializedViewHandler {
public:
    // Create a MaterializedViewHandler based on the catalog info and install it to the view table.
    MaterializedViewHandler(PersistentTable *targetTable,
                            catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                            VoltDBEngine *engine);
    ~MaterializedViewHandler();
    // We maintain the source table list here to register / de-register the view handler on the source tables.
    void addSourceTable(PersistentTable *sourceTable);
    void dropSourceTable(PersistentTable *sourceTable);
    PersistentTable *destTable() const { return m_destTable; }
    bool isDirty() { return m_dirty; }
    void pollute() { m_dirty = true; }
    void handleTupleInsert(PersistentTable *sourceTable, bool fallible);
    void handleTupleDelete(PersistentTable *sourceTable, bool fallible);

private:
    std::vector<PersistentTable*> m_sourceTables;
    PersistentTable *m_destTable;
    TableIndex *m_index;
    std::vector<boost::shared_ptr<ExecutorVector>> m_minMaxExecutorVectors;
    boost::shared_ptr<ExecutorVector> m_createQueryExecutorVector;
    int m_groupByColumnCount;
    int m_aggColumnCount;
    std::vector<ExpressionType> m_aggTypes;
    bool m_dirty;
    TableTuple m_existingTuple;
    TableTuple m_updatedTuple;
    StandAloneTupleStorage m_updatedTupleStorage;
    // vector of target table indexes to update.
    // Ideally, these should be a subset of the target table indexes that depend on the count and/or
    // aggregated columns, but there might be some other mostly harmless ones in there that are based
    // solely on the immutable primary key (GROUP BY columns).
    std::vector<TableIndex*> m_updatableIndexList;

    void install(PersistentTable *destTable,
                 catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                 VoltDBEngine *engine);
    void setUpAggregateInfo(catalog::MaterializedViewHandlerInfo *mvHandlerInfo);
    void setUpCreateQuery(catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                          VoltDBEngine *engine);
    void setUpMinMaxQueries(catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                            VoltDBEngine *engine);
    void setUpBackedTuples();
    void catchUpWithExistingData();
    bool findExistingTuple(const TableTuple &deltaTuple);
    void mergeTupleForInsert(const TableTuple &deltaTuple);
    void mergeTupleForDelete(const TableTuple &deltaTuple);
    NValue fallbackMinMaxColumn(int columnIndex, int minMaxColumnIndex);
};

} // namespace voltdb
#endif // MATERIALIZEDVIEWHANDLER_H_
