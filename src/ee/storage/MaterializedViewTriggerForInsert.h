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

#ifndef MATERIALIZEDVIEWTRIGGERFORINSERT_H_
#define MATERIALIZEDVIEWTRIGGERFORINSERT_H_

#include "catalog/catalogmap.h"
#include "catalog/materializedviewinfo.h"
#include "common/tabletuple.h"
#include "expressions/abstractexpression.h"
#include "MaterializedViewHandler.h"

#include "boost/foreach.hpp"
#include "boost/shared_array.hpp"

#include <string>
#include <vector>

namespace catalog {
class IndexRef;
class Statement;
}

namespace voltdb {

class AbstractExpression;
class ExecutorVector;
class PersistentTable;
class StreamedTable;
class Table;
class TableIndex;

/**
 * Manage the inserts and updates for a materialized view table based on inserts to
 * a source table. An instance sits between the two tables translating changes in one table
 * into changes in another table. It loads all this information from the catalog in its
 * constructor.
 */
class MaterializedViewTriggerForInsert {
public:
    virtual ~MaterializedViewTriggerForInsert();
    /**
     * Called when the source table is inserting a tuple. This will update the materialized view
     * destination table to reflect this change.
     */
    void processTupleInsert(const TableTuple &newTuple, bool fallible);

    PersistentTable * destTable() const { return m_dest; }

    catalog::MaterializedViewInfo* getMaterializedViewInfo() const {
        return m_mvInfo;
    }

    virtual void updateDefinition(PersistentTable *destTable, catalog::MaterializedViewInfo *mvInfo);

    template<class MATVIEW> static void segregateMaterializedViews(
        std::vector<MATVIEW*> &viewsIn,
        std::map<std::string, catalog::MaterializedViewInfo*>::const_iterator const & start,
        std::map<std::string, catalog::MaterializedViewInfo*>::const_iterator const & end,
        std::vector<catalog::MaterializedViewInfo*> &survivingInfosOut,
        std::vector<MATVIEW*> &survivingViewsOut,
        std::vector<MATVIEW*> &obsoleteViewsOut) {
        // iterate through all of the existing views
        BOOST_FOREACH(MATVIEW* currView, viewsIn) {
            const std::string& currentViewId = currView->destTable()->name();

            // iterate through all of the catalog views, looking for a match.
            std::map<std::string, catalog::MaterializedViewInfo*>::const_iterator viewIter;
            bool viewfound = false;
            for (viewIter = start; viewIter != end; ++viewIter) {
                catalog::MaterializedViewInfo* catalogViewInfo = viewIter->second;
                if (currentViewId == catalogViewInfo->name()) {
                    viewfound = true;
                    //TODO: This MIGHT be a good place to identify the need for view re-definition.
                    survivingInfosOut.push_back(catalogViewInfo);
                    survivingViewsOut.push_back(currView);
                    break;
                }
            }

            // If the table has a view that the catalog doesn't,
            // prepare to remove (or fail to migrate) the view.
            if (!viewfound) {
                obsoleteViewsOut.push_back(currView);
            }
        }
    }

    // Attempt to enable/disable the view.
    void setEnabled(bool value);

protected:
    MaterializedViewTriggerForInsert(PersistentTable *destTable,
                                  catalog::MaterializedViewInfo *mvInfo);
    void setDestTable(PersistentTable * dest);

    void initializeTupleHavingNoGroupBy(bool fallible);

    void allocateBackedTuples();

    void initUpdatableIndexList();

    /** load a predicate from the catalog structure if it's there */
    static AbstractExpression* parsePredicate(catalog::MaterializedViewInfo *mvInfo);
    std::size_t parseGroupBy(catalog::MaterializedViewInfo *mvInfo);
    std::size_t parseAggregation(catalog::MaterializedViewInfo *mvInfo);

    NValue getGroupByValueFromSrcTuple(int colIndex, const TableTuple& tuple);
    NValue getAggInputFromSrcTuple(int aggIndex, int aggExprOffset, const TableTuple& tuple);

    /**
     * build a search key based on the src table value
     * and use an index to find 0 or 1 rows in the view table
     */
    bool findExistingTuple(const TableTuple &oldTuple);
    // Find the existing tuple using a tuple from the delta table.
    bool findExistingTupleUsingDelta(const TableTuple &oldTuple);

    // space to store temp view tuples
    TableTuple m_existingTuple;
    TableTuple m_updatedTuple;
    boost::shared_array<char> m_updatedTupleBackingStore;
    TableTuple m_emptyTuple;
    boost::shared_array<char> m_emptyTupleBackingStore;

    // An optional predicate over source rows must pass for them to be included
    // in the materialized view.
    // This is a shared pointer to allow the views defined on the 'before' and
    // 'after' versions of a truncated source table to share the predicate
    // until the transaction ends, leaving only one of them.
    boost::shared_ptr<AbstractExpression> m_filterPredicate;

    // the materialized view table
    PersistentTable *m_dest;

    bool failsPredicate(const TableTuple& tuple) const {
        return (m_filterPredicate && !m_filterPredicate->eval(&tuple, NULL).isTrue());
    }

private:
    catalog::MaterializedViewInfo *m_mvInfo;

    // the primary index on the view table whose columns
    // are the same as the group by in the view query
    TableIndex *m_index;

    // storage to hold the value for the search key
    boost::shared_array<char> m_searchKeyBackingStore;

    std::vector<AbstractExpression *> m_groupByExprs;
    std::vector<int32_t> m_groupByColIndexes;
    // How many columns (or expressions) is the view aggregated on?
    // This MUST be declared/initialized AFTER m_groupByExprs/m_groupByColIndexes
    // but BEFORE m_searchKeyValues/m_searchKeyTuple/m_searchKeyBackingStore.

    void mergeTupleForInsert(const TableTuple &deltaTuple);

protected:
    std::size_t m_groupByColumnCount;
    std::vector<NValue> m_searchKeyValue;
    // space to hold the search key for the view table
    TableTuple m_searchKeyTuple;

    // what are the indexes of columns in the src table for
    // the columns in the view table
    std::vector<AbstractExpression *> m_aggExprs;
    std::vector<int32_t> m_aggColIndexes;
    // what are the aggregates for each column in the view table
    std::vector<ExpressionType> m_aggTypes;
    // How many optional agg columns in the materialized view table?
    // This MUST be declared/initialized AFTER m_aggExprs/m_aggColIndexes/m_aggTypes.
    std::size_t m_aggColumnCount;
    // Store the index of last COUNT(*) for optimization
    int m_countStarColumnIndex;

    // vector of target table indexes to update.
    // Ideally, these should be a subset of the target table indexes that
    // depend on the count and/or aggregated columns,
    // but there might be some other mostly harmless ones in there that are
    // based solely on the immutable primary key (GROUP BY columns).
    std::vector<TableIndex*> m_updatableIndexList;

    // Indicates whether the view can included in a snapshot.
    // If a view is partitioned but there is not an explicit partition column,
    // then it cannot be included in a snapshot.
    bool m_supportSnapshot;
    // Indicates whether the view is enabled.
    bool m_enabled;
};

/**
 * Manage the inserts and updates for a materialized view table based on inserts to a stream.
 * An instance sits between two tables translasting inserts in one table into
 * changes in the other table.
 * The factory method, build, uses information parsed from the catalog to configure
 * initializers for the private constructor.
 */
class MaterializedViewTriggerForStreamInsert : public  MaterializedViewTriggerForInsert {
public:
    static void build(StreamedTable *srcTable,
                      PersistentTable *destTable,
                      catalog::MaterializedViewInfo *mvInfo);
private:
    MaterializedViewTriggerForStreamInsert(PersistentTable *destTable,
                                           catalog::MaterializedViewInfo *mvInfo)
        : MaterializedViewTriggerForInsert(destTable, mvInfo)
    { }
};

} // namespace voltdb

#endif // MATERIALIZEDVIEWTRIGGERFORINSERT_H_
