/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#ifndef INDEXSTATS_H_
#define INDEXSTATS_H_

#include "stats/StatsSource.h"

namespace voltdb {
class TableIndex;
class TableTuple;
class TempTable;

/**
 * StatsSource extension for tables.
 */
class IndexStats : public StatsSource {
public:
    /**
     * Static method to generate the column names for the tables which
     * contain index stats.
     */
    static std::vector<std::string> generateIndexStatsColumnNames();

    /**
     * Static method to generate the remaining schema information for
     * the tables which contain index stats.
     */
    static void populateIndexStatsSchema(std::vector<voltdb::ValueType>& types,
                                         std::vector<int32_t>& columnLengths,
                                         std::vector<bool>& allowNull,
                                         std::vector<bool>& inBytes);

    static TempTable* generateEmptyIndexStatsTable();

    /*
     * Constructor caches reference to the table that will be generating the statistics
     */
    IndexStats(voltdb::TableIndex* index);

    ~IndexStats();

    /**
     * Configure a StatsSource superclass for a set of statistics. Since this class is only used in the EE it can be assumed that
     * it is part of an Execution Site and that there is a site Id.
     * @parameter name Name of this set of statistics
     * @parameter tableName Name of the indexed table
     */
    void configure(std::string name, std::string tableName);

    void rename(std::string name);

protected:

    /**
     * Update the stats tuple with the latest statistics available to this StatsSource.
     */
    virtual void updateStatsTuple(TableTuple *tuple);

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

private:
    /**
     * Index whose stats are being collected.
     */
    voltdb::TableIndex *m_index;

    voltdb::NValue m_indexName;
    voltdb::NValue m_indexType;

    int8_t m_isUnique;
    int8_t m_isCountable;

    int64_t m_lastTupleCount;
    int64_t m_lastMemEstimate;
};

}

#endif /* INDEXSTATS_H_ */
