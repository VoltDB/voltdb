/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
#include "storage/TableStats.h"
#include "stats/StatsSource.h"
#include "common/TupleSchema.h"
#include "common/ids.h"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "storage/table.h"
#include <vector>
#include <string>

namespace voltdb {
/*
 * Constructor caches reference to the table that will be generating the statistics
 */
TableStats::TableStats(voltdb::Table* table) : voltdb::StatsSource(), m_table(table) {
}

/**
 * Configure a StatsSource superclass for a set of statistics. Since this class is only used in the EE it can be assumed that
 * it is part of an Execution Site and that there is a site Id.
 * @parameter name Name of this set of statistics
 * @parameter siteId this stat source is associated with
 * @parameter databaseId database this source is associated with.
 */
void TableStats::configure(std::string name, voltdb::CatalogId siteId, voltdb::CatalogId databaseId) {
    StatsSource::configure(name, siteId, databaseId);
    m_tableName = ValueFactory::getStringValue(m_table->name());
    m_tableType = ValueFactory::getStringValue(m_table->tableType());
}

/**
 * Generates the list of column names that will be in the statTable_. Derived classes must override this method and call
 * the parent class's version to obtain the list of columns contributed by ancestors and then append the columns they will be
 * contributing to the end of the list.
 */
std::vector<std::string> TableStats::generateStatsColumnNames() {
    std::vector<std::string> columnNames = StatsSource::generateStatsColumnNames();
    columnNames.push_back("TABLE_NAME");
    columnNames.push_back("TABLE_TYPE");
    columnNames.push_back("TABLE_ACTIVE_TUPLE_COUNT");
    columnNames.push_back("TABLE_ALLOCATED_TUPLE_COUNT");
    columnNames.push_back("TABLE_DELETED_TUPLE_COUNT");
    return columnNames;
}

/**
 * Update the stats tuple with the latest statistics available to this StatsSource.
 */
void TableStats::updateStatsTuple(voltdb::TableTuple *tuple) {
    tuple->setNValue( StatsSource::m_columnName2Index["TABLE_NAME"], m_tableName);
    tuple->setNValue( StatsSource::m_columnName2Index["TABLE_TYPE"], m_tableType);
    tuple->setNValue( StatsSource::m_columnName2Index["TABLE_ACTIVE_TUPLE_COUNT"], ValueFactory::getBigIntValue(m_table->activeTupleCount()));
    tuple->setNValue( StatsSource::m_columnName2Index["TABLE_ALLOCATED_TUPLE_COUNT"], ValueFactory::getBigIntValue(m_table->allocatedTupleCount()));
    tuple->setNValue( StatsSource::m_columnName2Index["TABLE_DELETED_TUPLE_COUNT"], ValueFactory::getBigIntValue(m_table->deletedTupleCount()));
}

/**
 * Same pattern as generateStatsColumnNames except the return value is used as an offset into the tuple schema instead of appending to
 * end of a list.
 */
void TableStats::populateSchema(std::vector<voltdb::ValueType> &types, std::vector<uint16_t> &columnLengths, std::vector<bool> &allowNull) {
    StatsSource::populateSchema(types, columnLengths, allowNull);
    types.push_back(voltdb::VALUE_TYPE_VARCHAR); columnLengths.push_back(128); allowNull.push_back(false);
    types.push_back(voltdb::VALUE_TYPE_VARCHAR); columnLengths.push_back(128); allowNull.push_back(false);
    types.push_back(voltdb::VALUE_TYPE_BIGINT); columnLengths.push_back(static_cast<uint16_t>(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT))); allowNull.push_back(false);
    types.push_back(voltdb::VALUE_TYPE_BIGINT); columnLengths.push_back(static_cast<uint16_t>(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT))); allowNull.push_back(false);
    types.push_back(voltdb::VALUE_TYPE_BIGINT); columnLengths.push_back(static_cast<uint16_t>(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT))); allowNull.push_back(false);
}

TableStats::~TableStats() {
    m_tableName.free();
    m_tableType.free();
}

}
