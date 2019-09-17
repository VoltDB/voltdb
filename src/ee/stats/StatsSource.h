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

#include "common/tabletuple.h"
#include "common/ids.h"

#include <array>
#include <string>
#include <vector>
#include <map>

namespace voltdb {
class Table;
class TableFactory;
class TupleSchema;
class TableTuple;

/**
 * Abstract superclass of all sources of statistical information inside the EE. Statistics are currently represented as a single
 * row table that is updated every time it is retrieved.
 */
class StatsSource {
public:
    /**
     * column name, ValueType, column length, allow nulls, in bytes
     */
    using schema_tuple_type = std::tuple<std::string, ValueType, int32_t, bool, bool>;
    using schema_type = std::tuple<std::vector<std::string>,       // column name
          std::vector<ValueType>,          // column type
            std::vector<int32_t>,          // column length
            std::vector<bool>,             // allow null
            std::vector<bool>>;            // in bytes
    /**
     * Populates the other schema information which is present for
     * every stats table.  Usage by derived classes takes the same
     * pattern as generateBaseStatsColumnNames.
     */
    static schema_type populateBaseSchema();

    /*
     * Do nothing constructor that initializes statTable_ and schema_ to NULL.
     */
    StatsSource() = default;

    /**
     * Configure a StatsSource superclass for a set of statistics. Since this class is only used in the EE it can be assumed that
     * it is part of an Execution Site and that there is a site Id.
     * @parameter name Name of this set of statistics
     */
    void configure(std::string const& name);

    void updateTableName(const std::string& tableName);

    /*
     * Destructor that frees tupleSchema_, and statsTable_
     */
    virtual ~StatsSource();

    /**
     * Retrieve table containing the latest statistics available. An updated stat is requested from the derived class by calling
     * StatsSource::updateStatsTuple
     * @param interval Return counters since the beginning or since this method was last invoked
     * @param now Timestamp to return with each row
     * @return Pointer to a table containing the statistics.
     */
    Table* getStatsTable(int64_t siteId, int32_t partitionId, bool interval, int64_t now);

    /*
     * Retrieve tuple containing the latest statistics available. An updated stat is requested from the derived class by calling
     * StatsSource::updateStatsTuple
     * @param siteId for the generated tuple
     * @param partitionId for the generated tuple
     * @param interval Whether to return counters since the beginning or since the last time this was called
     * @param Timestamp to embed in each row
     * @return Pointer to a table tuple containing the latest version of the statistics.
     */
    TableTuple* getStatsTuple(int64_t siteId, int32_t partitionId, bool interval, int64_t now);

    /**
     * Retrieve the name of the table that this set of statistics is associated with.
     * @return Table name.
     */
    string getTableName() const;

    /**
     * String representation of the statistics. Default implementation is to print the stats table.
     * @return String representation
     */
    virtual std::string toString() const;
protected:
    /**
     * Update the stats tuple with the latest statistics available to this StatsSource. Implemented by derived classes.
     * @parameter tuple TableTuple pointing to a row in the stats table.
     */
    virtual void updateStatsTuple(TableTuple *tuple) = 0;
    static std::vector<std::string> generateStatsColumnNames();

    /**
     * Same pattern as generateStatsColumnNames except the return value is used as an offset into the tuple schema instead of appending to
     * end of a list.
     */
    virtual schema_type populateSchema();

    /**
     * Map describing the mapping from column names to column indices in the stats tuple. Necessary because classes in the
     * inheritance hierarchy can vary the number of columns they contribute. This removes the dependency between them.
     */
    std::map<std::string, int> m_columnName2Index;

    NValue m_tableName;

    bool interval() const {
        return m_interval;
    }
private:
    /**
     * Table containing the stat information. Shared pointer used as a substitute for scoped_ptr due to forward
     * declaration.
     */
    std::shared_ptr<Table> m_statsTable = nullptr;

    /**
     * Tuple used to modify the stat table.
     */
    TableTuple m_statsTuple;
    CatalogId m_hostId;
    NValue m_hostname;
    bool m_interval;
    static std::array<schema_tuple_type, 5> const BASE_SCHEMA;
};

}
