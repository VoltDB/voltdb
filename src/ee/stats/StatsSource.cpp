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

#include "stats/StatsSource.h"
#include "common/executorcontext.hpp"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"

using namespace voltdb;
using namespace std;

std::vector<std::string> const StatsSource::STATS_COLUMN_NAMES = {
    "TIMESTAMP", "HOST_ID", "HOSTNAME", "SITE_ID", "PARTITION_ID"
};

array<tuple<ValueType, int32_t, bool, bool>, 5> const StatsSource::BASE_SCHEMA = {
    make_tuple(ValueType::tBIGINT, NValue::getTupleStorageSize(ValueType::tBIGINT), false, false),
        make_tuple(ValueType::tINTEGER, NValue::getTupleStorageSize(ValueType::tINTEGER), false, false),
        make_tuple(ValueType::tVARCHAR, 4096,                                           false, false),
        make_tuple(ValueType::tINTEGER, NValue::getTupleStorageSize(ValueType::tINTEGER), false, false),
        make_tuple(ValueType::tBIGINT, NValue::getTupleStorageSize(ValueType::tBIGINT), false, false)
};

void StatsSource::populateBaseSchema(vector<ValueType>& types,
        vector<int32_t>& columnLengths,
        vector<bool>& allowNull,
        vector<bool>& inBytes) {
    for (auto const& t : BASE_SCHEMA) {
        types.emplace_back(get<0>(t));
        columnLengths.emplace_back(get<1>(t));
        allowNull.emplace_back(get<2>(t));
        inBytes.emplace_back(get<3>(t));
    }
}

/**
 * Configure a StatsSource superclass for a set of statistics. Since this class is only used in the EE it can be assumed that
 * it is part of an Execution Site and that there is a site Id.
 * @parameter name Name of this set of statistics
 * @parameter hostId id of the host this partition is on
 * @parameter hostname name of the host this partition is on
 * @parameter siteId this stat source is associated with
 * @parameter partitionId id of the partition assigned to this site
 */
void StatsSource::configure(string const& name) {
    ExecutorContext* executorContext = ExecutorContext::getExecutorContext();
    m_hostId = executorContext->m_hostId;
    m_hostname = ValueFactory::getStringValue(executorContext->m_hostname);

    vector<string> columnNames = generateStatsColumnNames();

    vector<ValueType> columnTypes;
    vector<int32_t> columnLengths;
    vector<bool> columnAllowNull;
    vector<bool> columnInBytes;
    populateSchema(columnTypes, columnLengths, columnAllowNull, columnInBytes);
    TupleSchema *schema = TupleSchema::createTupleSchema(
            columnTypes, columnLengths, columnAllowNull, columnInBytes);

    for (int ii = 0; ii < columnNames.size(); ii++) {
        m_columnName2Index[columnNames[ii]] = ii;
    }

    m_statsTable.reset(TableFactory::buildTempTable(name, schema, columnNames, nullptr));
    m_statsTuple = m_statsTable->tempTuple();
}

void StatsSource::updateTableName(const std::string& tableName) {
    m_tableName.free();
    m_tableName = ValueFactory::getStringValue(tableName);
}

StatsSource::~StatsSource() {
    m_hostname.free();
}

string StatsSource::getTableName() const {
    return m_tableName.toString();
}

/**
 * Retrieve table containing the latest statistics available. An updated stat is requested from the derived class by calling
 * StatsSource::updateStatsTuple
 * @param interval Return counters since the beginning or since this method was last invoked
 * @param now Timestamp to return with each row
 * @return Pointer to a table containing the statistics.
 */
Table* StatsSource::getStatsTable(int64_t siteId, int32_t partitionId, bool interval, int64_t now) {
    getStatsTuple(siteId, partitionId, interval, now);
    return m_statsTable.get();
}

/*
 * Retrieve tuple containing the latest statistics available. An updated stat is requested from the derived class by calling
 * StatsSource::updateStatsTuple
 * @param interval Whether to return counters since the beginning or since the last time this was called
 * @param Timestamp to embed in each row
 * @return Pointer to a table tuple containing the latest version of the statistics.
 */
TableTuple* StatsSource::getStatsTuple(int64_t siteId, int32_t partitionId, bool interval, int64_t now) {
    m_interval = interval;
    if (m_statsTable == nullptr) {
        VOLT_DEBUG("Table stats for site %" PRId64 ", partition %d is missing", siteId, partitionId);
        vassert(m_statsTable != nullptr);       // TODO: WTF???
        return nullptr;
    }
    m_statsTuple.setNValue(0, ValueFactory::getBigIntValue(now));
    m_statsTuple.setNValue(1, ValueFactory::getIntegerValue(static_cast<int32_t>(m_hostId)));
    m_statsTuple.setNValue(2, m_hostname);
    m_statsTuple.setNValue(3, ValueFactory::getIntegerValue(static_cast<int32_t>(siteId >> 32)));
    m_statsTuple.setNValue(4, ValueFactory::getBigIntValue(partitionId));
    updateStatsTuple(&m_statsTuple);
    // this was put in to collect history, but wasn't bounded so it leaked
    // also maybe better to collect history elsewhere
    //m_statsTable->insertTuple(m_statsTuple);

    return &m_statsTuple;
}

vector<string> StatsSource::generateStatsColumnNames() const {
    return generateBaseStatsColumnNames();
}

/**
 * String representation of the statistics. Default implementation is to print the stats table.
 * @return String representation
 */
string StatsSource::toString() const {
    string retString;
    for (int ii = 0; ii < m_statsTable->columnCount(); ii++) {
        retString.append(m_statsTable->columnName(ii)).append("\t");
    }
    return retString.append("\n")
        .append(m_statsTuple.debug(m_statsTable->name().c_str()));
}


void StatsSource::populateSchema(vector<ValueType>& types,
        vector<int32_t>& columnLengths,
        vector<bool>& allowNull,
        vector<bool>& inBytes) {
    populateBaseSchema(types, columnLengths, allowNull, inBytes);
}
