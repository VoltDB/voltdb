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

#ifndef TABLESTATS_H_
#define TABLESTATS_H_

#include "stats/StatsSource.h"
#include "common/TupleSchema.h"
#include "common/ids.h"
#include "storage/table.h"
#include <vector>
#include <string>

namespace voltdb {

/**
 * StatsSource extension for tables.
 */
class TableStats : public voltdb::StatsSource {
public:
    /**
     * Static method to generate the column names for the tables which
     * contain persistent table stats.
     */
    static std::vector<std::string> generateTableStatsColumnNames();

    /**
     * Static method to generate the remaining schema information for
     * the tables which contain persistent table stats.
     */
    static void populateTableStatsSchema(std::vector<voltdb::ValueType>& types,
                                         std::vector<int32_t>& columnLengths,
                                         std::vector<bool>& allowNull,
                                         std::vector<bool>& inBytes);

    /**
     * Return an empty TableStats table
     */
    static Table* generateEmptyTableStatsTable();

    /*
     * Constructor caches reference to the table that will be generating the statistics
     */
    TableStats(voltdb::Table* table);

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
    void configure(
            std::string name,
            voltdb::CatalogId databaseId);

protected:

    /**
     * Update the stats tuple with the latest statistics available to this StatsSource.
     */
    virtual void updateStatsTuple(voltdb::TableTuple *tuple);

    /**
     * Generates the list of column names that will be in the statTable_. Derived classes must override this method and call
     * the parent class's version to obtain the list of columns contributed by ancestors and then append the columns they will be
     * contributing to the end of the list.
     */
    virtual std::vector<std::string> generateStatsColumnNames();

    /**
     * Same pattern as generateStatsColumnNames except the return value is used as an offset into the tuple schema instead of appending to
     * end of a list.
     */
    virtual void populateSchema(std::vector<voltdb::ValueType> &types, std::vector<int32_t> &columnLengths,
            std::vector<bool> &allowNull, std::vector<bool> &inBytes);

    ~TableStats();

private:
    /**
     * Table whose stats are being collected.
     */
    voltdb::Table * m_table;

    voltdb::NValue m_tableName;

    voltdb::NValue m_tableType;

    int64_t m_lastTupleCount;
    int64_t m_lastAllocatedTupleMemory;
    int64_t m_lastOccupiedTupleMemory;
    int64_t m_lastStringDataMemory;
};

}

#endif /* TABLESTATS_H_ */
