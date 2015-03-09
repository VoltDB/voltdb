/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
#include "storage/TableStats.h"
#include "stats/StatsSource.h"
#include "common/TupleSchema.h"
#include "common/ids.h"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include <vector>
#include <string>
#include <math.h>

using namespace voltdb;
using namespace std;

vector<string> TableStats::generateTableStatsColumnNames() {
    vector<string> columnNames = StatsSource::generateBaseStatsColumnNames();
    columnNames.push_back("TABLE_NAME");
    columnNames.push_back("TABLE_TYPE");
    columnNames.push_back("TUPLE_COUNT");
    columnNames.push_back("TUPLE_ALLOCATED_MEMORY");
    columnNames.push_back("TUPLE_DATA_MEMORY");
    columnNames.push_back("STRING_DATA_MEMORY");
    columnNames.push_back("TUPLE_LIMIT");
    columnNames.push_back("PERCENT_FULL");
    return columnNames;
}

void TableStats::populateTableStatsSchema(
        vector<ValueType> &types,
        vector<int32_t> &columnLengths,
        vector<bool> &allowNull,
        vector<bool> &inBytes) {
    StatsSource::populateBaseSchema(types, columnLengths, allowNull, inBytes);
    types.push_back(VALUE_TYPE_VARCHAR); columnLengths.push_back(4096); allowNull.push_back(false);inBytes.push_back(false);
    types.push_back(VALUE_TYPE_VARCHAR); columnLengths.push_back(4096); allowNull.push_back(false);inBytes.push_back(false);
    types.push_back(VALUE_TYPE_BIGINT);  columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));  allowNull.push_back(false);inBytes.push_back(false);
    types.push_back(VALUE_TYPE_INTEGER); columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER)); allowNull.push_back(false);inBytes.push_back(false);
    types.push_back(VALUE_TYPE_INTEGER); columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER)); allowNull.push_back(false);inBytes.push_back(false);
    types.push_back(VALUE_TYPE_INTEGER); columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER)); allowNull.push_back(false);inBytes.push_back(false);
    types.push_back(VALUE_TYPE_INTEGER); columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER)); allowNull.push_back(false);inBytes.push_back(false);
    types.push_back(VALUE_TYPE_INTEGER); columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER)); allowNull.push_back(false);inBytes.push_back(false);
}

Table*
TableStats::generateEmptyTableStatsTable()
{
    string name = "Persistent Table aggregated table stats temp table";
    // An empty stats table isn't clearly associated with any specific
    // database ID.  Just pick something that works for now (Yes,
    // abstractplannode::databaseId(), I'm looking in your direction)
    CatalogId databaseId = 1;
    vector<string> columnNames = TableStats::generateTableStatsColumnNames();
    vector<ValueType> columnTypes;
    vector<int32_t> columnLengths;
    vector<bool> columnAllowNull;
    vector<bool> columnInBytes;
    TableStats::populateTableStatsSchema(columnTypes, columnLengths,
                                         columnAllowNull, columnInBytes);
    TupleSchema *schema =
        TupleSchema::createTupleSchema(columnTypes, columnLengths,
                                       columnAllowNull, columnInBytes);

    return
        reinterpret_cast<Table*>(TableFactory::getTempTable(databaseId,
                                                            name,
                                                            schema,
                                                            columnNames,
                                                            NULL));
}

/*
 * Constructor caches reference to the table that will be generating the statistics
 */
TableStats::TableStats(Table* table)
    : StatsSource(), m_table(table), m_lastTupleCount(0),
      m_lastAllocatedTupleMemory(0), m_lastOccupiedTupleMemory(0),
      m_lastStringDataMemory(0)
{
}

/**
 * Configure a StatsSource superclass for a set of statistics. Since this class is only used in the EE it can be assumed that
 * it is part of an Execution Site and that there is a site Id.
 * @parameter name Name of this set of statistics
 * @parameter hostId id of the host this partition is on
 * @parameter hostname name of the host this partition is on
 * @parameter siteId this stat source is associated with
 * @parameter partitionId this stat source is associated with
 * @parameter databaseId Database this source is associated with
 */
void TableStats::configure(
        string name,
        CatalogId databaseId) {
    StatsSource::configure(name, databaseId);
    m_tableName = ValueFactory::getStringValue(m_table->name());
    m_tableType = ValueFactory::getStringValue(m_table->tableType());
}

/**
 * Generates the list of column names that will be in the statTable_. Derived classes must override this method and call
 * the parent class's version to obtain the list of columns contributed by ancestors and then append the columns they will be
 * contributing to the end of the list.
 */
vector<string> TableStats::generateStatsColumnNames() {
    return TableStats::generateTableStatsColumnNames();
}

/**
 * Update the stats tuple with the latest statistics available to this StatsSource.
 */
void TableStats::updateStatsTuple(TableTuple *tuple) {
    tuple->setNValue( StatsSource::m_columnName2Index["TABLE_NAME"], m_tableName);
    tuple->setNValue( StatsSource::m_columnName2Index["TABLE_TYPE"], m_tableType);
    int64_t tupleCount = m_table->activeTupleCount();
    int tupleLimit = m_table->tupleLimit();
    // This overflow is unlikely (requires 2 terabytes of allocated string memory)
    int64_t allocated_tuple_mem_kb = m_table->allocatedTupleMemory() / 1024;
    int64_t occupied_tuple_mem_kb = 0;
    if (!m_table->isExport()) {
        occupied_tuple_mem_kb = m_table->occupiedTupleMemory() / 1024;
    }
    int64_t string_data_mem_kb = m_table->nonInlinedMemorySize() / 1024;

    if (interval()) {
        tupleCount = tupleCount - m_lastTupleCount;
        m_lastTupleCount = m_table->activeTupleCount();
        allocated_tuple_mem_kb =
            allocated_tuple_mem_kb - (m_lastAllocatedTupleMemory / 1024);
        m_lastAllocatedTupleMemory = m_table->allocatedTupleMemory();
        occupied_tuple_mem_kb =
            occupied_tuple_mem_kb - (m_lastOccupiedTupleMemory / 1024);
        m_lastOccupiedTupleMemory = m_table->occupiedTupleMemory();
        string_data_mem_kb =
            string_data_mem_kb - (m_lastStringDataMemory / 1024);
        m_lastStringDataMemory = m_table->nonInlinedMemorySize();
    }

    if (string_data_mem_kb > INT32_MAX)
    {
        string_data_mem_kb = -1;
    }
    if (allocated_tuple_mem_kb > INT32_MAX)
    {
        allocated_tuple_mem_kb = -1;
    }
    if (occupied_tuple_mem_kb > INT32_MAX)
    {
        occupied_tuple_mem_kb = -1;
    }

    tuple->setNValue(
            StatsSource::m_columnName2Index["TUPLE_COUNT"],
            ValueFactory::getBigIntValue(tupleCount));
    tuple->setNValue(StatsSource::m_columnName2Index["TUPLE_ALLOCATED_MEMORY"],
            ValueFactory::getIntegerValue(static_cast<int32_t>(allocated_tuple_mem_kb)));
    tuple->setNValue(StatsSource::m_columnName2Index["TUPLE_DATA_MEMORY"],
            ValueFactory::getIntegerValue(static_cast<int32_t>(occupied_tuple_mem_kb)));
    tuple->setNValue(StatsSource::m_columnName2Index["STRING_DATA_MEMORY"],
            ValueFactory::getIntegerValue(static_cast<int32_t>(string_data_mem_kb)));

    bool hasTupleLimit = tupleLimit == INT_MAX ? false : true;
    tuple->setNValue(StatsSource::m_columnName2Index["TUPLE_LIMIT"],
            hasTupleLimit ? ValueFactory::getIntegerValue(tupleLimit): ValueFactory::getNullValue());
    int32_t percentage = 0;
    if (hasTupleLimit && tupleLimit > 0) {
        percentage = static_cast<int32_t> (ceil(static_cast<double>(tupleCount) * 100.0 / tupleLimit));
    }
    tuple->setNValue(StatsSource::m_columnName2Index["PERCENT_FULL"],ValueFactory::getIntegerValue(percentage));
}

/**
 * Same pattern as generateStatsColumnNames except the return value is used as an offset into the tuple schema instead of appending to
 * end of a list.
 */
void TableStats::populateSchema(
        vector<ValueType> &types,
        vector<int32_t> &columnLengths,
        vector<bool> &allowNull,
        vector<bool> &inBytes) {
    TableStats::populateTableStatsSchema(types, columnLengths, allowNull, inBytes);
}

TableStats::~TableStats() {
    m_tableName.free();
    m_tableType.free();
}
