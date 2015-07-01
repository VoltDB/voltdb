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

#ifndef STATSSOURCE_H_
#define STATSSOURCE_H_

#include "common/tabletuple.h"
#include "common/ids.h"
#include "boost/scoped_ptr.hpp"

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
     * Generates the list of column names that are present for every
     * stats table.  Derived classes should implement their own static
     * methods to generate their column names and call this method
     * within it to populate the column name vector before adding
     * their stat-specific column names.
     */
    static std::vector<std::string> generateBaseStatsColumnNames();

    /**
     * Populates the other schema information which is present for
     * every stats table.  Usage by derived classes takes the same
     * pattern as generateBaseStatsColumnNames.
     */
    static void populateBaseSchema(std::vector<voltdb::ValueType>& types,
                                   std::vector<int32_t>& columnLengths,
                                   std::vector<bool>& allowNull,
                                   std::vector<bool>& inBytes);

    /*
     * Do nothing constructor that initializes statTable_ and schema_ to NULL.
     */
    StatsSource();

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
    voltdb::Table* getStatsTable(bool interval, int64_t now);

    /*
     * Retrieve tuple containing the latest statistics available. An updated stat is requested from the derived class by calling
     * StatsSource::updateStatsTuple
     * @param interval Whether to return counters since the beginning or since the last time this was called
     * @param Timestamp to embed in each row
     * @return Pointer to a table tuple containing the latest version of the statistics.
     */
    voltdb::TableTuple* getStatsTuple(bool interval, int64_t now);

    /**
     * Retrieve the name of this set of statistics
     * @return Name of statistics
     */
    std::string getName();

    /**
     * String representation of the statistics. Default implementation is to print the stats table.
     * @return String representation
     */
    virtual std::string toString();
protected:
    /**
     * Update the stats tuple with the latest statistics available to this StatsSource. Implemented by derived classes.
     * @parameter tuple TableTuple pointing to a row in the stats table.
     */
    virtual void updateStatsTuple(voltdb::TableTuple *tuple) = 0;

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

    /**
     * Map describing the mapping from column names to column indices in the stats tuple. Necessary because classes in the
     * inheritance hierarchy can vary the number of columns they contribute. This removes the dependency between them.
     */
    std::map<std::string, int> m_columnName2Index;

    bool interval() { return m_interval; }
private:

    /**
     * Table containing the stat information. Shared pointer used as a substitute for scoped_ptr due to forward
     * declaration.
     */
    boost::scoped_ptr<voltdb::Table> m_statsTable;

    /**
     * Tuple used to modify the stat table.
     */
    voltdb::TableTuple m_statsTuple;

    /**
     * Name of this set of statistics.
     */
    std::string m_name;

    /**
     * CatalogId of the partition this StatsSource is associated with.
     */
    voltdb::CatalogId m_partitionId;

    int64_t m_siteId;
    voltdb::CatalogId m_hostId;

    voltdb::NValue m_hostname;

    bool m_interval;

};

}
#endif /* STATSCONTAINER_H_ */
