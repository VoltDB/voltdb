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

#include <vector>
#include <string>
#include "indexes/IndexStats.h"
#include "stats/StatsSource.h"
#include "common/TupleSchema.h"
#include "common/ids.h"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "indexes/tableindex.h"

using namespace voltdb;
using namespace std;

vector<string> IndexStats::generateIndexStatsColumnNames() {
    vector<string> columnNames = StatsSource::generateBaseStatsColumnNames();
    columnNames.push_back("INDEX_NAME");
    columnNames.push_back("TABLE_NAME");
    columnNames.push_back("INDEX_TYPE");
    columnNames.push_back("IS_UNIQUE");
    columnNames.push_back("IS_COUNTABLE");
    columnNames.push_back("ENTRY_COUNT");
    columnNames.push_back("MEMORY_ESTIMATE");

    return columnNames;
}

void IndexStats::populateIndexStatsSchema(
        vector<ValueType> &types,
        vector<int32_t> &columnLengths,
        vector<bool> &allowNull,
        vector<bool> &inBytes) {
    StatsSource::populateBaseSchema(types, columnLengths, allowNull, inBytes);

    // index name
    types.push_back(VALUE_TYPE_VARCHAR);
    columnLengths.push_back(4096); // This means if user's index name length exceed 4096, problem may happen.
    allowNull.push_back(false);
    inBytes.push_back(false);

    // table name
    types.push_back(VALUE_TYPE_VARCHAR);
    columnLengths.push_back(4096);
    allowNull.push_back(false);
    inBytes.push_back(false);

    // index type
    types.push_back(VALUE_TYPE_VARCHAR);
    columnLengths.push_back(4096);
    allowNull.push_back(false);
    inBytes.push_back(false);

    // is unique
    types.push_back(VALUE_TYPE_TINYINT);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
    allowNull.push_back(false);
    inBytes.push_back(false);

    // is countable
    types.push_back(VALUE_TYPE_TINYINT);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
    allowNull.push_back(false);
    inBytes.push_back(false);

    // entry count
    types.push_back(VALUE_TYPE_BIGINT);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
    allowNull.push_back(false);
    inBytes.push_back(false);

    // memory usage
    types.push_back(VALUE_TYPE_INTEGER);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
    allowNull.push_back(false);
    inBytes.push_back(false);
}

Table*
IndexStats::generateEmptyIndexStatsTable()
{
    string name = "Persistent Table aggregated index stats temp table";
    // An empty stats table isn't clearly associated with any specific
    // database ID.  Just pick something that works for now (Yes,
    // abstractplannode::databaseId(), I'm looking in your direction)
    CatalogId databaseId = 1;
    vector<string> columnNames = IndexStats::generateIndexStatsColumnNames();
    vector<ValueType> columnTypes;
    vector<int32_t> columnLengths;
    vector<bool> columnAllowNull;
    vector<bool> columnInBytes;
    IndexStats::populateIndexStatsSchema(columnTypes, columnLengths,
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
IndexStats::IndexStats(TableIndex* index)
    : StatsSource(), m_index(index), m_isUnique(0), m_isCountable(0),
      m_lastTupleCount(0), m_lastMemEstimate(0)
{
}

/**
 * Configure a StatsSource superclass for a set of statistics. Since this class is only used in
 * the EE it can be assumed that it is part of an Execution Site and that there is a site Id.
 * @parameter name Name of this set of statistics
 * @parameter hostId id of the host this partition is on
 * @parameter hostname name of the host this partition is on
 * @parameter siteId this stat source is associated with
 * @parameter partitionId this stat source is associated with
 * @parameter databaseId Database this source is associated with
 */
void IndexStats::configure(
        string name,
        string tableName,
        CatalogId databaseId) {
    StatsSource::configure(name, databaseId);
    m_indexName = ValueFactory::getStringValue(m_index->getName());
    m_tableName = ValueFactory::getStringValue(tableName);
    m_indexType = ValueFactory::getStringValue(m_index->getTypeName());
    m_isUnique = static_cast<int8_t>(m_index->isUniqueIndex() ? 1 : 0);
    m_isCountable = static_cast<int8_t>(m_index->isCountableIndex() ? 1 : 0);
}

void IndexStats::rename(std::string name) {
    m_indexName.free();
    m_indexName = ValueFactory::getStringValue(name);
}

/**
 * Generates the list of column names that will be in the statTable_. Derived classes must override
 * this method and call the parent class's version to obtain the list of columns contributed by
 * ancestors and then append the columns they will be contributing to the end of the list.
 */
vector<string> IndexStats::generateStatsColumnNames()
{
    return IndexStats::generateIndexStatsColumnNames();
}

/**
 * Update the stats tuple with the latest statistics available to this StatsSource.
 */
void IndexStats::updateStatsTuple(TableTuple *tuple) {
    tuple->setNValue( StatsSource::m_columnName2Index["INDEX_NAME"], m_indexName);
    tuple->setNValue( StatsSource::m_columnName2Index["TABLE_NAME"], m_tableName);
    tuple->setNValue( StatsSource::m_columnName2Index["INDEX_TYPE"], m_indexType);
    int64_t count = static_cast<int64_t>(m_index->getSize());
    int64_t mem_estimate_kb = m_index->getMemoryEstimate() / 1024;

    if (interval()) {
        count = count - m_lastTupleCount;
        m_lastTupleCount = static_cast<int64_t>(m_index->getSize());
        mem_estimate_kb = mem_estimate_kb - (m_lastMemEstimate / 1024);
        m_lastMemEstimate = m_index->getMemoryEstimate();
    }

    if (mem_estimate_kb > INT32_MAX)
    {
        mem_estimate_kb = -1;
    }

    tuple->setNValue(
            StatsSource::m_columnName2Index["IS_UNIQUE"],
            ValueFactory::getTinyIntValue(m_isUnique));
    tuple->setNValue(
                StatsSource::m_columnName2Index["IS_COUNTABLE"],
                ValueFactory::getTinyIntValue(m_isCountable));
    tuple->setNValue( StatsSource::m_columnName2Index["ENTRY_COUNT"],
            ValueFactory::getBigIntValue(count));
    tuple->setNValue(StatsSource::m_columnName2Index["MEMORY_ESTIMATE"],
                     ValueFactory::
                     getIntegerValue(static_cast<int32_t>(mem_estimate_kb)));
}

/**
 * Same pattern as generateStatsColumnNames except the return value is used as an offset into
 * the tuple schema instead of appending to end of a list.
 */
void IndexStats::populateSchema(
        vector<ValueType> &types,
        vector<int32_t> &columnLengths,
        vector<bool> &allowNull,
        vector<bool> &inBytes)
{
    IndexStats::populateIndexStatsSchema(types, columnLengths, allowNull, inBytes);
}

IndexStats::~IndexStats() {
    m_indexName.free();
    m_indexType.free();
    m_tableName.free();
}
