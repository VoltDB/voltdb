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

#include "common/types.h"
#include "common/ValueFactory.hpp"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "stats/StatsSource.h"
#include <vector>
#include <string>
#include <cassert>

namespace voltdb {

StatsSource::StatsSource()  : m_statsTable(NULL) {
}

/**
 * Configure a StatsSource superclass for a set of statistics. Since this class is only used in the EE it can be assumed that
 * it is part of an Execution Site and that there is a site Id.
 * @parameter name Name of this set of statistics
 * @parameter hostId id of the host this partition is on
 * @parameter hostname name of the host this partition is on
 * @parameter siteId this stat source is associated with
 * @parameter partitionId id of the partition assigned to this site
 * @parameter databaseId database this source is associated with.
 */
void StatsSource::configure(
        std::string name,
        voltdb::CatalogId hostId,
        std::string hostname,
        voltdb::CatalogId siteId,
        voltdb::CatalogId partitionId,
        voltdb::CatalogId databaseId) {
    m_siteId = siteId;
    m_partitionId = partitionId;
    m_hostId = hostId;
    m_hostname = ValueFactory::getStringValue(hostname);
    std::vector<std::string> columnNames = generateStatsColumnNames();

    std::vector<voltdb::ValueType> columnTypes;
    std::vector<uint16_t> columnLengths;
    std::vector<bool> columnAllowNull;
    populateSchema(columnTypes, columnLengths, columnAllowNull);
    TupleSchema *schema = voltdb::TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, true);

    for (int ii = 0; ii < columnNames.size(); ii++) {
        m_columnName2Index[columnNames[ii]] = ii;
    }

    m_statsTable.reset(voltdb::TableFactory::getTempTable(databaseId, name, schema, &columnNames[0], NULL));
    m_statsTuple = m_statsTable->tempTuple();
}

StatsSource::~StatsSource() {
    m_hostname.free();
}

/**
 * Retrieve the name of this set of statistics
 * @return Name of statistics
 */
std::string StatsSource::getName() {
    return m_name;
}

/**
 * Retrieve table containing the latest statistics available. An updated stat is requested from the derived class by calling
 * StatsSource::updateStatsTuple
 * @param interval Return counters since the beginning or since this method was last invoked
 * @param now Timestamp to return with each row
 * @return Pointer to a table containing the statistics.
 */
voltdb::Table* StatsSource::getStatsTable(bool interval, int64_t now) {
    getStatsTuple(interval, now);
    return m_statsTable.get();
}

/*
 * Retrieve tuple containing the latest statistics available. An updated stat is requested from the derived class by calling
 * StatsSource::updateStatsTuple
 * @param interval Whether to return counters since the beginning or since the last time this was called
 * @param Timestamp to embed in each row
 * @return Pointer to a table tuple containing the latest version of the statistics.
 */
voltdb::TableTuple* StatsSource::getStatsTuple(bool interval, int64_t now) {
    m_interval = interval;
    assert (m_statsTable != NULL);
    if (m_statsTable == NULL) {
        return NULL;
    }
    m_statsTuple.setNValue(0, ValueFactory::getBigIntValue(m_hostId));
    m_statsTuple.setNValue(1, m_hostname);
    m_statsTuple.setNValue(2, ValueFactory::getBigIntValue(m_siteId));
    m_statsTuple.setNValue(3, ValueFactory::getBigIntValue(m_partitionId));
    m_statsTuple.setNValue(4, ValueFactory::getBigIntValue(now));
    updateStatsTuple(&m_statsTuple);
    m_statsTable->insertTuple(m_statsTuple);
    //assert (success);
    return &m_statsTuple;
}

/**
 * Generates the list of column names that will be in the statTable_. Derived classes must override this method and call
 * the parent class's version to obtain the list of columns contributed by ancestors and then append the columns they will be
 * contributing to the end of the list.
 */
std::vector<std::string> StatsSource::generateStatsColumnNames() {
    std::vector<std::string> columnNames;
    columnNames.push_back("HOST_ID");
    columnNames.push_back("HOSTNAME");
    columnNames.push_back("SITE_ID");
    columnNames.push_back("PARTITION_ID");
    columnNames.push_back("TIMESTAMP");
    return columnNames;
}

/**
 * String representation of the statistics. Default implementation is to print the stats table.
 * @return String representation
 */
std::string StatsSource::toString() {
    std::string retString = "";
    for (int ii = 0; ii < m_statsTable->columnCount(); ii++) {
        retString += m_statsTable->columnName(ii);
        retString += "\t";
    }
    retString += "\n";
    retString += m_statsTuple.debug(m_statsTable->name().c_str());
    return retString;
}


/**
 * Same pattern as generateStatsColumnNames except the return value is used as an offset into the tuple schema instead of appending to
 * end of a list.
 */
void StatsSource::populateSchema(std::vector<voltdb::ValueType> &types, std::vector<uint16_t> &columnLengths, std::vector<bool> &allowNull) {
    types.push_back(voltdb::VALUE_TYPE_BIGINT); columnLengths.push_back(static_cast<uint16_t>(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT))); allowNull.push_back(false);
    types.push_back(voltdb::VALUE_TYPE_VARCHAR); columnLengths.push_back(4096); allowNull.push_back(false);
    types.push_back(voltdb::VALUE_TYPE_BIGINT); columnLengths.push_back(static_cast<uint16_t>(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT))); allowNull.push_back(false);
    types.push_back(voltdb::VALUE_TYPE_BIGINT); columnLengths.push_back(static_cast<uint16_t>(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT))); allowNull.push_back(false);
    types.push_back(voltdb::VALUE_TYPE_BIGINT); columnLengths.push_back(static_cast<uint16_t>(NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT))); allowNull.push_back(false);
}

}
