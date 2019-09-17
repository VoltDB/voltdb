/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "indexes/tableindex.h"

using namespace voltdb;
using namespace std;

array<tuple<string, ValueType, int32_t, bool, bool>, 7> const IndexStats::BASE_SCHEMA = {
    make_tuple("INDEX_NAME", ValueType::tVARCHAR, 4096, false, false),
    make_tuple("TABLE_NAME", ValueType::tVARCHAR, 4096, false, false),
    make_tuple("INDEX_TYPE", ValueType::tVARCHAR, 4096, false, false),

    make_tuple("IS_UNIQUE",    ValueType::tTINYINT, NValue::getTupleStorageSize(ValueType::tTINYINT), false, false),
    make_tuple("IS_COUNTABLE", ValueType::tTINYINT, NValue::getTupleStorageSize(ValueType::tTINYINT), false, false),

    make_tuple("ENTRY_COUNT",     ValueType::tBIGINT, NValue::getTupleStorageSize(ValueType::tBIGINT), false, false),
    make_tuple("MEMORY_ESTIMATE", ValueType::tBIGINT, NValue::getTupleStorageSize(ValueType::tBIGINT), false, false)
};

vector<string> IndexStats::generateIndexStatsColumnNames() {
    vector<string> columnNames = StatsSource::generateBaseStatsColumnNames();
    for (auto const& t : BASE_SCHEMA) {
        columnNames.emplace_back(get<0>(t));
    }
    return columnNames;
}

// make sure to update schema in frontend sources (like IndexStats.java) and tests when updating
// the index-stats schema in here.
void IndexStats::populateIndexStatsSchema(
        vector<ValueType> &types,
        vector<int32_t> &columnLengths,
        vector<bool> &allowNull,
        vector<bool> &inBytes) {
    StatsSource::populateBaseSchema(types, columnLengths, allowNull, inBytes);
    for (auto const& t : BASE_SCHEMA) {
        types.emplace_back(get<1>(t));
        columnLengths.emplace_back(get<2>(t));
        allowNull.emplace_back(get<3>(t));
        inBytes.emplace_back(get<4>(t));
    }
}

TempTable* IndexStats::generateEmptyIndexStatsTable() {
    string name = "Persistent Table aggregated index stats temp table";
    // An empty stats table isn't clearly associated with any specific
    // database ID.  Just pick something that works for now (Yes,
    // abstractplannode::databaseId(), I'm looking in your direction)
    vector<string> columnNames = generateIndexStatsColumnNames();
    vector<ValueType> columnTypes;
    vector<int32_t> columnLengths;
    vector<bool> columnAllowNull;
    vector<bool> columnInBytes;
    populateIndexStatsSchema(columnTypes, columnLengths, columnAllowNull, columnInBytes);
    TupleSchema *schema = TupleSchema::createTupleSchema(
            columnTypes, columnLengths, columnAllowNull, columnInBytes);
    return TableFactory::buildTempTable(name, schema, columnNames, nullptr);
}

/*
 * Constructor caches reference to the table that will be generating the statistics
 */
IndexStats::IndexStats(TableIndex* index) : StatsSource(), m_index(index) {}

/**
 * Configure a StatsSource superclass for a set of statistics. Since this class is only used in
 * the EE it can be assumed that it is part of an Execution Site and that there is a site Id.
 * @parameter name Name of this set of statistics
 * @parameter tableName Name of the indexed table
 */
void IndexStats::configure(string const& name, string const& tableName) {
    VOLT_TRACE("Configuring stats for index %s in table %s.", name.c_str(), tableName.c_str());
    StatsSource::configure(name);
    m_indexName = ValueFactory::getStringValue(m_index->getName());
    m_tableName = ValueFactory::getStringValue(tableName);
    m_indexType = ValueFactory::getStringValue(m_index->getTypeName());
    m_isUnique = m_index->isUniqueIndex() ? 1 : 0;
    m_isCountable = m_index->isCountableIndex() ? 1 : 0;
}

void IndexStats::rename(std::string const& name) {
    m_indexName.free();
    m_indexName = ValueFactory::getStringValue(name);
}

/**
 * Generates the list of column names that will be in the statTable_. Derived classes must override
 * this method and call the parent class's version to obtain the list of columns contributed by
 * ancestors and then append the columns they will be contributing to the end of the list.
 */
vector<string> IndexStats::generateStatsColumnNames() const {
    return generateIndexStatsColumnNames();
}

/**
 * Update the stats tuple with the latest statistics available to this StatsSource.
 */
void IndexStats::updateStatsTuple(TableTuple *tuple) {
    tuple->setNValue(StatsSource::m_columnName2Index["INDEX_NAME"], m_indexName);
    tuple->setNValue(StatsSource::m_columnName2Index["TABLE_NAME"], m_tableName);
    tuple->setNValue(StatsSource::m_columnName2Index["INDEX_TYPE"], m_indexType);
    int64_t count = static_cast<int64_t>(m_index->getSize());
    int64_t mem_estimate_kb = m_index->getMemoryEstimate() / 1024;

    if (interval()) {
        count -= m_lastTupleCount;
        m_lastTupleCount = static_cast<int64_t>(m_index->getSize());
        mem_estimate_kb = mem_estimate_kb - (m_lastMemEstimate / 1024);
        m_lastMemEstimate = m_index->getMemoryEstimate();
    }
    tuple->setNValue(StatsSource::m_columnName2Index["IS_UNIQUE"],
            ValueFactory::getTinyIntValue(m_isUnique));
    tuple->setNValue(StatsSource::m_columnName2Index["IS_COUNTABLE"],
            ValueFactory::getTinyIntValue(m_isCountable));
    tuple->setNValue(StatsSource::m_columnName2Index["ENTRY_COUNT"],
            ValueFactory::getBigIntValue(count));
    tuple->setNValue(StatsSource::m_columnName2Index["MEMORY_ESTIMATE"],
            ValueFactory::getBigIntValue(mem_estimate_kb));
}

/**
 * Same pattern as generateStatsColumnNames except the return value is used as an offset into
 * the tuple schema instead of appending to end of a list.
 */
void IndexStats::populateSchema(
        vector<ValueType> &types,
        vector<int32_t> &columnLengths,
        vector<bool> &allowNull,
        vector<bool> &inBytes) {
    populateIndexStatsSchema(types, columnLengths, allowNull, inBytes);
}

IndexStats::~IndexStats() {
    m_indexName.free();
    m_indexType.free();
    m_tableName.free();
}
