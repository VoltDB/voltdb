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

#ifndef MATERIALIZEDVIEWMETADATA_H_
#define MATERIALIZEDVIEWMETADATA_H_

#include <vector>

#include "common/types.h"
#include "common/tabletuple.h"
#include "indexes/tableindex.h"
#include "catalog/materializedviewinfo.h"

namespace voltdb {

class AbstractExpression;
class PersistentTable;
class TableIndex;

/**
 * Manage the inserts, deletes and updates for a materialized view table based on changes to
 * a source table. An instance sits between the two tables translasting changes in one table
 * into changes in another table. It loads all this information from the catalog in its
 * constructor.
 */
class MaterializedViewMetadata {
public:

    MaterializedViewMetadata(PersistentTable *srcTable, PersistentTable *destTable, catalog::MaterializedViewInfo *mvInfo);
    ~MaterializedViewMetadata();

    /**
     * Called when the source table is inserting a tuple. This will update the materialized view
     * destination table to reflect this change.
     */
    void processTupleInsert(const TableTuple &newTuple, bool fallible);

    /**
     * Called when the source table is deleting a tuple. This will update the materialized view
     * destination table to reflect this change.
     */
    void processTupleDelete(const TableTuple &oldTuple, bool fallible);

    PersistentTable * targetTable() const { return m_target; }
    std::string indexForMinMax() const { return m_indexForMinMax == NULL ? "" : m_indexForMinMax->getName(); }

    void setTargetTable(PersistentTable * target);
    void setIndexForMinMax(std::string index);

    catalog::MaterializedViewInfo* getMaterializedViewInfo() {
        return m_mvInfo;
    }
private:

    void freeBackedTuples();
    void allocateBackedTuples();

    /** load a predicate from the catalog structure if it's there */
    static AbstractExpression* parsePredicate(catalog::MaterializedViewInfo *mvInfo);

    std::size_t parseGroupBy(catalog::MaterializedViewInfo *mvInfo);
    std::size_t parseAggregation(catalog::MaterializedViewInfo *mvInfo);
    NValue getGroupByValueFromSrcTuple(int colIndex, const TableTuple& tuple);
    NValue getAggInputFromSrcTuple(int aggIndex, const TableTuple& tuple);

    /**
     * build a search key based on the src table value
     * and use an index to find 0 or 1 rows in the view table
     */
    bool findExistingTuple(const TableTuple &oldTuple);

    NValue findMinMaxFallbackValueIndexed(const TableTuple& oldTuple,
                                          const NValue &existingValue,
                                          const NValue &initialNull,
                                          int negate_for_min,
                                          int aggIndex);

    NValue findMinMaxFallbackValueSequential(const TableTuple& oldTuple,
                                             const NValue &existingValue,
                                             const NValue &initialNull,
                                             int negate_for_min,
                                             int aggIndex);

    // the source persistent table
    PersistentTable *m_srcTable;
    // the materialized view table
    PersistentTable *m_target;

    catalog::MaterializedViewInfo *m_mvInfo;

    // the primary index on the view table whose columns
    // are the same as the group by in the view query
    TableIndex *m_index;

    // the index on srcTable which can be used to maintain min/max
    TableIndex *m_indexForMinMax;

    // space to store temp view tuples
    TableTuple m_existingTuple;
    TableTuple m_updatedTuple;
    char *m_updatedTupleBackingStore;
    TableTuple m_emptyTuple;
    char *m_emptyTupleBackingStore;

    // predicate to include or exclude rows from being
    // part of the aggregation in the materialized view
    AbstractExpression *m_filterPredicate;

    std::vector<AbstractExpression *> m_groupByExprs;
    std::vector<int32_t> m_groupByColIndexes;
    // How many columns (or expressions) is the view aggregated on?
    // This MUST be declared/initialized AFTER m_groupByExprs/m_groupByColIndexes
    // but BEFORE m_searchKeyValues/m_searchKeyTuple/m_searchKeyBackingStore.
    std::size_t m_groupByColumnCount;
    std::vector<NValue> m_searchKeyValue;
    // space to hold the search key for the view table
    TableTuple m_searchKeyTuple;
    // storage to hold the value for the search key
    char *m_searchKeyBackingStore;

    // which columns in the source table

    // what are the indexes of columns in the src table for
    // the columns in the view table
    std::vector<AbstractExpression *> m_aggExprs;
    std::vector<int32_t> m_aggColIndexes;
    // what are the aggregates for each column in the view table
    std::vector<ExpressionType> m_aggTypes;
    // How many optional agg columns in the materialized view table?
    // This MUST be declared/initialized AFTER m_aggExprs/m_aggColIndexes/m_aggTypes.
    std::size_t m_aggColumnCount;

    // vector of target table indexes to update.
    // Ideally, these should be a subset of the target table indexes that depend on the count and/or
    // aggregated columns, but there might be some other mostly harmless ones in there that are based
    // solely on the immutable primary key (GROUP BY columns).
    std::vector<TableIndex*> m_updatableIndexList;
};

} // namespace voltdb

#endif // MATERIALIZEDVIEWMETADATA_H_
