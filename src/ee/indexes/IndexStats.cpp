/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

#include <vector>
#include <string>
#include "indexes/IndexStats.h"
#include "stats/StatsSource.h"
#include "common/TupleSchema.h"
#include "common/ids.h"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "indexes/tableindex.h"

namespace voltdb {
/*
 * Constructor caches reference to the table that will be generating the statistics
 */
IndexStats::IndexStats(TableIndex* index) : voltdb::StatsSource(), m_index(index),
        m_isUnique(0), m_lastTupleCount(0) {
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
void IndexStats::configure(
        std::string name,
        voltdb::CatalogId hostId,
        std::string hostname,
        voltdb::CatalogId siteId,
        voltdb::CatalogId partitionId,
        voltdb::CatalogId databaseId) {
    StatsSource::configure(name, hostId, hostname, siteId, partitionId, databaseId);
    m_indexName = ValueFactory::getStringValue(m_index->getName());
    m_indexType = ValueFactory::getStringValue("Unknown");
}

/**
 * Generates the list of column names that will be in the statTable_. Derived classes must override this method and call
 * the parent class's version to obtain the list of columns contributed by ancestors and then append the columns they will be
 * contributing to the end of the list.
 */
std::vector<std::string> IndexStats::generateStatsColumnNames() {
    std::vector<std::string> columnNames = StatsSource::generateStatsColumnNames();
    columnNames.push_back("INDEX_NAME");
    columnNames.push_back("INDEX_TYPE");
    columnNames.push_back("INDEX_UNIQUE");
    columnNames.push_back("INDEX_KEY_COUNT");
    return columnNames;
}

/**
 * Update the stats tuple with the latest statistics available to this StatsSource.
 */
void IndexStats::updateStatsTuple(voltdb::TableTuple *tuple) {
    tuple->setNValue( StatsSource::m_columnName2Index["INDEX_NAME"], m_indexName);
    tuple->setNValue( StatsSource::m_columnName2Index["INDEX_TYPE"], m_indexType);
    int64_t count = static_cast<int64_t>(m_index->getSize());

    if (interval()) {
        count = count - m_lastTupleCount;
        m_lastTupleCount = static_cast<int64_t>(m_index->getSize());
    }

    tuple->setNValue(
            StatsSource::m_columnName2Index["INDEX_UNIQUE"],
            ValueFactory::getTinyIntValue(0));
    tuple->setNValue( StatsSource::m_columnName2Index["INDEX_KEY_COUNT"],
            ValueFactory::getBigIntValue(count));
}

/**
 * Same pattern as generateStatsColumnNames except the return value is used as an offset into the tuple schema instead of appending to
 * end of a list.
 */
void IndexStats::populateSchema(
        std::vector<voltdb::ValueType> &types,
        std::vector<int32_t> &columnLengths,
        std::vector<bool> &allowNull) {
    StatsSource::populateSchema(types, columnLengths, allowNull);
    types.push_back(voltdb::VALUE_TYPE_VARCHAR); columnLengths.push_back(4096); allowNull.push_back(false);
    types.push_back(voltdb::VALUE_TYPE_VARCHAR); columnLengths.push_back(4096); allowNull.push_back(false);
    types.push_back(voltdb::VALUE_TYPE_TINYINT); columnLengths.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_TINYINT)); allowNull.push_back(false);
    types.push_back(voltdb::VALUE_TYPE_BIGINT); columnLengths.push_back(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT)); allowNull.push_back(false);
}

IndexStats::~IndexStats() {
    m_indexName.free();
    m_indexType.free();
}

}
