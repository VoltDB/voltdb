/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#include <sstream>
#include "tablefactory.h"
#include "common/executorcontext.hpp"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/persistenttable.h"
#include "storage/streamedtable.h"
#include "storage/temptable.h"
#include "storage/TempTableLimits.h"
#include "indexes/tableindexfactory.h"
#include "common/Pool.hpp"

namespace voltdb {
Table* TableFactory::getPersistentTable(
            voltdb::CatalogId databaseId,
            const std::string &name,
            TupleSchema* schema,
            const std::vector<std::string> &columnNames,
            char *signature,
            bool tableIsMaterialized,
            int partitionColumn,
            bool exportEnabled,
            bool exportOnly,
            int tableAllocationTargetSize,
            int tupleLimit,
            int32_t compactionThreshold,
            bool drEnabled)
{
    Table *table = NULL;

    if (exportOnly) {
        table = new StreamedTable(exportEnabled);
    }
    else {
        table = new PersistentTable(partitionColumn, signature, tableIsMaterialized, tableAllocationTargetSize, tupleLimit, drEnabled);
    }

    initCommon(databaseId,
               table,
               name,
               schema,
               columnNames,
               true,  // table will take ownership of TupleSchema object
               compactionThreshold);

    // initialize stats for the table
    configureStats(databaseId, name, table);

    /*if(!exportOnly) {
        // allocate tuple storage block for the persistent table ahead of time
        // instead of waiting till first tuple insertion. Intend of allocating tuple
        // block storage ahead is to improve performance on first tuple insertion.
        PersistentTable *persistentTable = static_cast<PersistentTable*>(table);
        TBPtr block = persistentTable->allocateNextBlock();
        assert(block->hasFreeTuples());
        persistentTable->m_blocksWithSpace.insert(block);
        std::cout << "getPersistentTable(); "<<std::endl;
    }*/

    return table;
}

TempTable* TableFactory::getTempTable(
            const voltdb::CatalogId databaseId,
            const std::string &name,
            TupleSchema* schema,
            const std::vector<std::string> &columnNames,
            TempTableLimits* limits)
{
    TempTable* table = new TempTable();
    initCommon(databaseId, table, name, schema, columnNames, true);
    table->m_limits = limits;
    return table;
}

/**
 * Creates a temp table with the same schema as the provided template table
 */
TempTable* TableFactory::getCopiedTempTable(
            const voltdb::CatalogId databaseId,
            const std::string &name,
            const Table* template_table,
            TempTableLimits* limits)
{
    TempTable* table = new TempTable();
    initCommon(databaseId, table, name, template_table->m_schema, template_table->m_columnNames, false);
    table->m_limits = limits;
    return table;
}

void TableFactory::initCommon(
            voltdb::CatalogId databaseId,
            Table *table,
            const std::string &name,
            TupleSchema *schema,
            const std::vector<std::string> &columnNames,
            const bool ownsTupleSchema,
            const int32_t compactionThreshold) {

    assert(table != NULL);
    assert(schema != NULL);
    assert(columnNames.size() != 0);

    table->m_databaseId = databaseId;
    table->m_name = name;
    table->initializeWithColumns(schema, columnNames, ownsTupleSchema, compactionThreshold);
    assert (table->columnCount() == schema->columnCount());
}

void TableFactory::configureStats(voltdb::CatalogId databaseId,
                                  std::string name,
                                  Table *table) {
    // initialize stats for the table
    table->getTableStats()->configure(name + " stats",
                                      databaseId);
}

}
