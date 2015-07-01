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

#include "stats/StatsSource.h"

#include "common/executorcontext.hpp"
#include "common/types.h"
#include "common/ValueFactory.hpp"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include <vector>
#include <string>
#include <cassert>

using namespace voltdb;
using namespace std;

vector<string> StatsSource::generateBaseStatsColumnNames() {
    vector<string> columnNames;
    columnNames.push_back("TIMESTAMP");
    columnNames.push_back("HOST_ID");
    columnNames.push_back("HOSTNAME");
    columnNames.push_back("SITE_ID");
    columnNames.push_back("PARTITION_ID");
    return columnNames;
}

void StatsSource::populateBaseSchema(vector<ValueType> &types, vector<int32_t> &columnLengths,
        vector<bool> &allowNull, vector<bool> &inBytes) {
    types.push_back(VALUE_TYPE_BIGINT); columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT)); allowNull.push_back(false);inBytes.push_back(false);
    types.push_back(VALUE_TYPE_INTEGER); columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER)); allowNull.push_back(false);inBytes.push_back(false);
    types.push_back(VALUE_TYPE_VARCHAR); columnLengths.push_back(4096); allowNull.push_back(false);inBytes.push_back(false);
    types.push_back(VALUE_TYPE_INTEGER); columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER)); allowNull.push_back(false);inBytes.push_back(false);
    types.push_back(VALUE_TYPE_BIGINT); columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT)); allowNull.push_back(false);inBytes.push_back(false);
}

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
        string name,
        CatalogId databaseId) {
    ExecutorContext* executorContext = ExecutorContext::getExecutorContext();
    m_siteId = executorContext->m_siteId;
    m_partitionId = executorContext->m_partitionId;
    m_hostId = executorContext->m_hostId;
    m_hostname = ValueFactory::getStringValue(executorContext->m_hostname);

    vector<string> columnNames = generateStatsColumnNames();

    vector<ValueType> columnTypes;
    vector<int32_t> columnLengths;
    vector<bool> columnAllowNull;
    vector<bool> columnInBytes;
    populateSchema(columnTypes, columnLengths, columnAllowNull, columnInBytes);
    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, columnInBytes);

    for (int ii = 0; ii < columnNames.size(); ii++) {
        m_columnName2Index[columnNames[ii]] = ii;
    }

    m_statsTable.reset(TableFactory::getTempTable(databaseId, name, schema, columnNames, NULL));
    m_statsTuple = m_statsTable->tempTuple();
}

StatsSource::~StatsSource() {
    m_hostname.free();
}

/**
 * Retrieve the name of this set of statistics
 * @return Name of statistics
 */
string StatsSource::getName() {
    return m_name;
}

/**
 * Retrieve table containing the latest statistics available. An updated stat is requested from the derived class by calling
 * StatsSource::updateStatsTuple
 * @param interval Return counters since the beginning or since this method was last invoked
 * @param now Timestamp to return with each row
 * @return Pointer to a table containing the statistics.
 */
Table* StatsSource::getStatsTable(bool interval, int64_t now) {
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
TableTuple* StatsSource::getStatsTuple(bool interval, int64_t now) {
    m_interval = interval;
    assert (m_statsTable != NULL);
    if (m_statsTable == NULL) {
        return NULL;
    }
    m_statsTuple.setNValue(0, ValueFactory::getBigIntValue(now));
    m_statsTuple.setNValue(1, ValueFactory::getIntegerValue(static_cast<int32_t>(m_hostId)));
    m_statsTuple.setNValue(2, m_hostname);
    m_statsTuple.setNValue(3, ValueFactory::getIntegerValue(static_cast<int32_t>(m_siteId >> 32)));
    m_statsTuple.setNValue(4, ValueFactory::getBigIntValue(m_partitionId));
    updateStatsTuple(&m_statsTuple);

    // this was put in to collect history, but wasn't bounded so it leaked
    // also maybe better to collect history elsewhere
    //m_statsTable->insertTuple(m_statsTuple);

    return &m_statsTuple;
}

/**
 * Generates the list of column names that will be in the statTable_. Derived classes must override this method and call
 * the parent class's version to obtain the list of columns contributed by ancestors and then append the columns they will be
 * contributing to the end of the list.
 */
vector<string> StatsSource::generateStatsColumnNames()
{
    return StatsSource::generateBaseStatsColumnNames();
}

/**
 * String representation of the statistics. Default implementation is to print the stats table.
 * @return String representation
 */
string StatsSource::toString() {
    string retString = "";
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
void
StatsSource::populateSchema(vector<ValueType> &types, vector<int32_t> &columnLengths,
        vector<bool> &allowNull, vector<bool> &inBytes) {
    StatsSource::populateBaseSchema(types, columnLengths, allowNull, inBytes);
}
