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

#pragma once

#include <array>
#include "stats/StatsSource.h"

namespace voltdb {
class Table;
class TableTuple;
class TempTable;

/**
 * StatsSource extension for tables.
 */
class TableStats : public StatsSource {
    /**
     * Table whose stats are being collected.
     */
    Table* m_table;
    NValue m_tableType;
    int64_t m_lastTupleCount = 0;
    int64_t m_lastAllocatedTupleMemory = 0;
    int64_t m_lastOccupiedTupleMemory = 0;
    int64_t m_lastStringDataMemory = 0;

    static std::array<schema_tuple_type, 8> const BASE_SCHEMA;
    /**
     * Static method to generate the column names for the tables which
     * contain persistent table stats.
     */
    static std::vector<std::string> generateStatsColumnNames();
public:

    /**
     * Static method to generate the remaining schema information for
     * the tables which contain persistent table stats.
     */
    static schema_type populateTableStatsSchema();

    /**
     * Return an empty TableStats table
     */
    static TempTable* generateEmptyTableStatsTable();

    /*
     * Constructor caches reference to the table that will be generating the statistics
     */
    TableStats(Table* table);
    ~TableStats();

    /**
     * Configure a StatsSource superclass for a set of statistics. Since this class is only used in the EE it can be assumed that
     * it is part of an Execution Site and that there is a site Id.
     * @parameter name Name of this set of statistics
     */
    void configure(std::string const& name);
protected:

    /**
     * Update the stats tuple with the latest statistics available to this StatsSource.
     */
    void updateStatsTuple(TableTuple *tuple) override;

    /**
     * Same pattern as generateStatsColumnNames except the return value is used as an offset into the tuple schema instead of appending to
     * end of a list.
     */
    schema_type populateSchema() override;
};

}

