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

#include <sstream>

#include "tablefactory.h"
#include "storage/LargeTempTable.h"
#include "storage/streamedtable.h"
#include "storage/temptable.h"
#include "indexes/tableindexfactory.h"

namespace voltdb {
Table* TableFactory::getPersistentTable(
        voltdb::CatalogId databaseId, char const* name, TupleSchema* schema,
            const std::vector<std::string> &columnNames, char *signature, bool tableIsMaterialized,
            int partitionColumn, TableType tableType, int tableAllocationTargetSize,
            int32_t compactionThreshold, bool drEnabled, bool isReplicated) {
    Table *table = NULL;
    StreamedTable *streamedTable = NULL;
    PersistentTable *persistentTable = NULL;

    if (tableTypeIsStream(tableType)) {
        table = streamedTable = new StreamedTable(partitionColumn, isReplicated);
    } else {
        table = persistentTable = new PersistentTable(
                partitionColumn, signature, tableIsMaterialized, tableAllocationTargetSize,
                drEnabled, isReplicated, tableType);
    }

    initCommon(databaseId, table, name, schema, columnNames,
               true,  // table will take ownership of TupleSchema object
               compactionThreshold);

    TableStats *stats;
    if (tableTypeIsStream(tableType)) {
        stats = streamedTable->getTableStats();
    } else {
        stats = persistentTable->getTableStats();
        // Allocate and assign the tuple storage block to the persistent table ahead of time instead
        // of doing so at time of first tuple insertion. The intent of block allocation ahead of time
        // is to avoid allocation cost at time of tuple insertion
        TBPtr block = persistentTable->allocateFirstBlock();
        vassert(block->hasFreeTuples());
        persistentTable->m_blocksWithSpace.insert(block);
    }

    // initialize stats for the table
    configureStats(name, stats);

    // If a regular table with export enabled, create a companion streamed table
    if (isTableWithStream(tableType)) {
        streamedTable = new StreamedTable(partitionColumn, isReplicated);
        initCommon(databaseId, streamedTable, name, schema, columnNames,
                   false,  // companion streamed table will NOT take ownership of TupleSchema object
                   compactionThreshold);
        persistentTable->setStreamedTable(streamedTable);
        VOLT_TRACE("Created companion streamed table for %s", persistentTable->name().c_str());
    }

    return table;
}

// This is a convenient wrapper for test only.
StreamedTable* TableFactory::getStreamedTableForTest(
            voltdb::CatalogId databaseId, const std::string &name, TupleSchema* schema,
            const std::vector<std::string> &columnNames, ExportTupleStream* wrapper,
            bool exportEnabled, int32_t compactionThreshold) {
    StreamedTable *table = new StreamedTable(wrapper);

    initCommon(databaseId, table, name, schema, columnNames,
               true,  // table will take ownership of TupleSchema object
               compactionThreshold);

    // initialize stats for the table
    configureStats(name.c_str(), table->getTableStats());

    return table;
}

TempTable* TableFactory::buildTempTable(const std::string &name, TupleSchema* schema,
        const std::vector<std::string> &columnNames, TempTableLimits const* limits) {
    TempTable* table = new TempTable();
    initCommon(0, table, name, schema, columnNames, true);
    table->m_limits = const_cast<TempTableLimits*>(limits);
    return table;
}

/**
 * Creates a temp table with the same schema as the provided template table
 */
AbstractTempTable* TableFactory::buildCopiedTempTable(
            const std::string &name, const Table* template_table, const ExecutorVector& executorVector) {
    AbstractTempTable* newTable = NULL;
    if (executorVector.isLargeQuery()) {
        newTable = new LargeTempTable();
    } else {
        TempTable* newTempTable = new TempTable();
        newTempTable->m_limits = const_cast<TempTableLimits*>(executorVector.limits());
        newTable = newTempTable;
    }

    initCommon(0, newTable, name, template_table->m_schema, template_table->m_columnNames, false);
    return newTable;
}

TempTable* TableFactory::buildCopiedTempTable(const std::string &name, const Table* template_table) {
    TempTable* newTable = new TempTable();
    initCommon(0, newTable, name, template_table->m_schema, template_table->m_columnNames, false);
    return newTable;
}

LargeTempTable* TableFactory::buildCopiedLargeTempTable( const std::string &name, const Table* templateTable) {
    LargeTempTable* newTable = new LargeTempTable();
    initCommon(0, newTable, name, templateTable->m_schema, templateTable->m_columnNames, false);
    return newTable;
}

LargeTempTable* TableFactory::buildLargeTempTable(const std::string &name, TupleSchema* schema,
        const std::vector<std::string> &columnNames) {
    LargeTempTable* table = new LargeTempTable();
    initCommon(0, table, name, schema, columnNames, true);
    return table;
}

void TableFactory::initCommon(voltdb::CatalogId databaseId, Table *table,
        const std::string &name, TupleSchema *schema, const std::vector<std::string> &columnNames,
        const bool ownsTupleSchema, const int32_t compactionThreshold) {
    vassert(table != NULL);
    vassert(schema != NULL);
    vassert(columnNames.size() != 0);

    table->m_databaseId = databaseId;
    table->m_name = name;
    table->initializeWithColumns(schema, columnNames, ownsTupleSchema, compactionThreshold);
    vassert(table->columnCount() == schema->columnCount());
}

void TableFactory::configureStats(char const* name, TableStats *tableStats) {
    // initialize stats for the table
    tableStats->configure(std::string(name).append(" stats"));
}

}
