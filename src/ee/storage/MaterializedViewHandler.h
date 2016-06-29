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
#include "execution/ExecutorVector.h"
#include "persistenttable.h"

namespace voltdb {

class VoltDBEngine;

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
    void handleTupleInsert(PersistentTable *sourceTable);
    void handleTupleDelete(PersistentTable *sourceTable);

private:
    std::vector<PersistentTable*> m_sourceTables;
    PersistentTable *m_destTable;
    std::vector<boost::shared_ptr<ExecutorVector>> m_minMaxExecutorVectors;
    boost::shared_ptr<ExecutorVector> m_createQueryExecutorVector;
    std::vector<int32_t> m_columnTypes;
    bool m_dirty;

    void install(PersistentTable *destTable,
                 catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                 VoltDBEngine *engine);
    void setUpCreateQuery(catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                          VoltDBEngine *engine);
    void setUpMinMaxQueries(catalog::MaterializedViewHandlerInfo *mvHandlerInfo,
                            VoltDBEngine *engine);
};

} // namespace voltdb
#endif // MATERIALIZEDVIEWHANDLER_H_
