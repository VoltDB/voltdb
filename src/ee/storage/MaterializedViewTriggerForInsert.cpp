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
#include "MaterializedViewTriggerForInsert.h"

#include "persistenttable.h"
#include "streamedtable.h"

#include "catalog/column.h"
#include "catalog/columnref.h"
#include "catalog/table.h"
#include "expressions/expressionutil.h"
#include "indexes/tableindex.h"

ENABLE_BOOST_FOREACH_ON_CONST_MAP(Statement);
typedef std::pair<std::string, catalog::Statement*> LabeledStatement;

using namespace std;
namespace voltdb {

MaterializedViewTriggerForInsert::MaterializedViewTriggerForInsert(PersistentTable *destTable,
                                                                   catalog::MaterializedViewInfo *mvInfo)
    : m_filterPredicate(parsePredicate(mvInfo))
    , m_dest(destTable)
    , m_index(destTable->primaryKeyIndex())
    , m_groupByColumnCount(parseGroupBy(mvInfo)) // also loads m_groupByExprs/Columns as needed
    , m_searchKeyValue(m_groupByColumnCount)
    , m_aggColumnCount(parseAggregation(mvInfo))
    , m_supportSnapshot(true)
    , m_enabled(true) {
    VOLT_TRACE("Construct MaterializedViewTriggerForInsert...");

    m_mvInfo = mvInfo;
    const catalog::Table* catalogDestTable = mvInfo->dest();
    if (! catalogDestTable->isreplicated() && catalogDestTable->partitioncolumn() == NULL) {
        // If the destination table is partitioned but there is no partition column,
        // we cannot snapshot this view.
        m_supportSnapshot = false;
    }

    // best not to have to worry about the destination table disappearing
    // out from under the source table that feeds it.
    m_dest->incrementRefcount();
    m_dest->setMaterializedViewTrigger(this);

    // When updateTupleWithSpecificIndexes needs to be called,
    // the context is lost that identifies which base table columns potentially changed.
    // So the minimal set of indexes that MIGHT need to be updated must include
    // any that are not solely based on primary key components.
    // Until the DDL compiler does this analysis and marks the indexes accordingly,
    // include all dest table indexes except the actual primary key index on the group by columns.
    initUpdatableIndexList();
    allocateBackedTuples();

    VOLT_TRACE("Finished MaterializedViewTriggerForInsert initialization...");
}

MaterializedViewTriggerForInsert::~MaterializedViewTriggerForInsert() {
    BOOST_FOREACH (auto groupByExpr, m_groupByExprs) {
        delete groupByExpr;
    }
    BOOST_FOREACH (auto aggExpr, m_aggExprs) {
        delete aggExpr;
    }
    m_dest->setMaterializedViewTrigger(NULL);
    m_dest->decrementRefcount();
}

void MaterializedViewTriggerForInsert::setEnabled(bool enabled) {
    if (! m_supportSnapshot) {
        // If this view should not respond to any view status toggle requests
        // (because the view is implicitly partitioned), ignore them.
        return;
    } else if (m_enabled == enabled) { // If the value is not changed, no action needs to be taken.
        return;
    }
    // Only views that can be snapshotted are allowed to be disabled.
    m_enabled = enabled;
    // We already ensured this in its parent call.
    const bool noNeedToCheckMemoryContext = false;
    if (! m_enabled && ! m_dest->isPersistentTableEmpty()) {
        // If the view maintenance is disabled, and the view is not empty,
        // we need to use a delta table to hold the view content restored from
        // the snapshot and do a manual merge afterwards.
        m_dest->instantiateDeltaTable(noNeedToCheckMemoryContext);
    } else if (m_enabled && m_dest->deltaTable()) {
        // When we turn on the maintenance, if a delta table exists, it means that the view table was
        // not empty at the time when we paused it.
        // In this case, we need to do a merge. Log a message for it.
        char msg[256];
        snprintf(msg, sizeof(msg), "Merging the pre-existing content in view %s with the snapshot data.",
                 m_dest->name().c_str());
        msg[sizeof msg - 1] = '\0';
        LogManager::getThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_INFO, msg);

        PersistentTable* delta = m_dest->deltaTable();
        TableIterator ti = delta->iterator();
        TableTuple deltaTuple(delta->schema());
        while (ti.next(deltaTuple)) {
            // Notice that here we are passing view table tuples, not source table tuples like we do
            // in processTupleInsert() and processTupleDelete().
            // To differentiate that, we use findExistingTupleUsingDelta() instead of findExistingTuple().
            bool found = findExistingTupleUsingDelta(deltaTuple);
            if (found) {
                mergeTupleForInsert(deltaTuple);
                // Shouldn't need to update group-key-only indexes such as the primary key
                // since their keys shouldn't ever change, but do update other indexes.
                m_dest->updateTupleWithSpecificIndexes(
                      m_existingTuple, m_updatedTuple, m_updatableIndexList, false);
            }
            else {
                m_dest->insertPersistentTuple(deltaTuple, false);
            }
        }
        // The way we are currently using to call this function for replicated tables is already synchronized.
        // Only the lowest site should be instantiating and releasing the delta table.
        m_dest->releaseDeltaTable(noNeedToCheckMemoryContext);
    }
}

void MaterializedViewTriggerForInsert::mergeTupleForInsert(const TableTuple &deltaTuple) {
    // set up the group-by columns
    for (int colindex = 0; colindex < m_groupByColumnCount; colindex++) {
        // note that if the tuple is in the mv's target table,
        // tuple values should be pulled from the existing tuple in
        // that table. This works around a memory ownership issue
        // related to out-of-line strings.
        NValue value = m_existingTuple.getNValue(colindex);
        m_updatedTuple.setNValue(colindex, value);
    }
    // Aggregations
    int aggOffset = m_groupByColumnCount;
    for (int aggIndex = 0, columnIndex = aggOffset; aggIndex < m_aggColumnCount; aggIndex++, columnIndex++) {
        NValue existingValue = m_existingTuple.getNValue(columnIndex);
        NValue newValue = deltaTuple.getNValue(columnIndex);
        if (newValue.isNull()) {
            newValue = existingValue;
        } else {
            switch(m_aggTypes[aggIndex]) {
                case EXPRESSION_TYPE_AGGREGATE_SUM:
                case EXPRESSION_TYPE_AGGREGATE_COUNT:
                case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR:
                    if (!existingValue.isNull()) {
                        newValue = existingValue.op_add(newValue);
                    }
                    break;
                case EXPRESSION_TYPE_AGGREGATE_MIN:
                    // ignore any new value that is not strictly an improvement
                    if (!existingValue.isNull() && newValue.compare(existingValue) >= 0) {
                        newValue = existingValue;
                    }
                    break;
                case EXPRESSION_TYPE_AGGREGATE_MAX:
                    // ignore any new value that is not strictly an improvement
                    if (!existingValue.isNull() && newValue.compare(existingValue) <= 0) {
                        newValue = existingValue;
                    }
                    break;
                default:
                    vassert(false); // Should have been caught when the matview was loaded.
                    // no break
            }
        }
        m_updatedTuple.setNValue(columnIndex, newValue);
    }
    // Copy any migrating information
    int migIndex = m_dest->getMigrateColumnIndex();
    if (migIndex != TupleSchema::UNSET_HIDDEN_COLUMN) {
        m_updatedTuple.setHiddenNValue(migIndex, m_existingTuple.getHiddenNValue(migIndex));
    }
}

void MaterializedViewTriggerForInsert::initUpdatableIndexList() {
    // Note that if the way we initialize this m_updatableIndexList changes in the future,
    //   we will also need to change the condition to detect when the m_updatableIndexList
    //   should be refreshed in the updateDefinition() function.
    const std::vector<TableIndex*>& destIndexes = m_dest->allIndexes();
    m_updatableIndexList.clear();
    BOOST_FOREACH (auto index, destIndexes) {
        if (index != m_index) {
            m_updatableIndexList.push_back(index);
        }
    }
}

void MaterializedViewTriggerForInsert::updateDefinition(PersistentTable *destTable, catalog::MaterializedViewInfo *mvInfo) {
    setDestTable(destTable);
    initUpdatableIndexList();
}

// numCountStar is needed because COUNT(*) is not part of m_aggExprs
NValue MaterializedViewTriggerForInsert::getAggInputFromSrcTuple(
      int aggIndex, int numCountStar, const TableTuple& tuple) {
    if (m_aggExprs.size() != 0) {
        AbstractExpression* aggExpr = m_aggExprs[aggIndex - numCountStar];
        return aggExpr->eval(&tuple, NULL);
    }

    int srcColIdx = m_aggColIndexes[aggIndex];
    return tuple.getNValue(srcColIdx);
}

void MaterializedViewTriggerForInsert::processTupleInsert(const TableTuple &newTuple, bool fallible) {
    // If the view is not enabled, ignore it.
    // Snapshots will only do inserts, so this check is not added to handleTupleDelete.
    if (! m_enabled) {
        return;
    } else if (failsPredicate(newTuple)) {
       // don't change the view if this tuple doesn't match the predicate
        return;
    }

    bool const exists = findExistingTuple(newTuple);
    if (!exists) {
        // create a blank tuple
        VOLT_TRACE("newTuple does not exist, create a blank tuple");
        m_existingTuple.move(m_emptyTuple.address());
    }
    // clear the tuple that will be built to insert or overwrite
    memset(m_updatedTuple.address(), 0, m_dest->getTupleLength());

    // set up the first n columns, based on group-by columns
    for (int colindex = 0; colindex < m_groupByColumnCount; colindex++) {
        // note that if the tuple is in the mv's dest table,
        // tuple values should be pulled from the existing tuple in
        // that table. This works around a memory ownership issue
        // related to out-of-line strings.
        NValue value = exists ? m_existingTuple.getNValue(colindex) : m_searchKeyValue[colindex];
        m_updatedTuple.setNValue(colindex, value);
    }

    int aggOffset = (int)m_groupByColumnCount;
    // m_aggExprs has complex aggregation operations which does not include COUNT(*)
    int numCountStar = 0;
    // set values for the other columns
    // update or insert the row
    if (exists) {
        for (int aggIndex = 0; aggIndex < m_aggColumnCount; aggIndex++) {
            NValue existingValue = m_existingTuple.getNValue(aggOffset+aggIndex);
            if (m_aggTypes[aggIndex] == EXPRESSION_TYPE_AGGREGATE_COUNT_STAR) {
                m_updatedTuple.setNValue( (int)(aggOffset+aggIndex),
                                 m_existingTuple.getNValue( (int)(aggOffset+aggIndex) ).op_increment());
                numCountStar++;
                continue;
            }

            // get new value for all other aggregate ops other than COUNT(*)
            NValue newValue = getAggInputFromSrcTuple(aggIndex, numCountStar, newTuple);
            if (newValue.isNull()) {
                newValue = existingValue;
            } else {
                switch(m_aggTypes[aggIndex]) {
                   case EXPRESSION_TYPE_AGGREGATE_SUM:
                       if (!existingValue.isNull()) {
                          newValue = existingValue.op_add(newValue);
                       }
                       break;
                   case EXPRESSION_TYPE_AGGREGATE_COUNT:
                       newValue = existingValue.op_increment();
                       break;
                   case EXPRESSION_TYPE_AGGREGATE_MIN:
                       // ignore any new value that is not strictly an improvement
                       if (!existingValue.isNull() && newValue.compare(existingValue) >= 0) {
                          newValue = existingValue;
                       }
                       break;
                   case EXPRESSION_TYPE_AGGREGATE_MAX:
                       // ignore any new value that is not strictly an improvement
                       if (!existingValue.isNull() && newValue.compare(existingValue) <= 0) {
                          newValue = existingValue;
                       }
                       break;
                   default:
                       vassert(false); // Should have been caught when the matview was loaded.
               }
            }
            m_updatedTuple.setNValue(aggOffset+aggIndex, newValue);
        }
        // ENG-10892, if no COUNT(*) column exists
        if (numCountStar == 0) {
            // check which hidden column COUNT(*) lies in: assume same index in both tables
            vassert(m_dest->hasViewCountColumn());
            int colIndex = m_dest->getViewCountColumnIndex();
            m_updatedTuple.setHiddenNValue(
                    colIndex, m_existingTuple.getHiddenNValue(colIndex).op_increment());
        }
        // Copy any migrating information
        int migIndex = m_dest->getMigrateColumnIndex();
        if (migIndex != TupleSchema::UNSET_HIDDEN_COLUMN) {
            m_updatedTuple.setHiddenNValue(migIndex, m_existingTuple.getHiddenNValue(migIndex));
        }
        // Shouldn't need to update group-key-only indexes such as the primary key
        // since their keys shouldn't ever change, but do update other indexes.
        m_dest->updateTupleWithSpecificIndexes(m_existingTuple, m_updatedTuple, m_updatableIndexList, fallible);
    } else {
        int numCountStar = 0;
        // A new group row gets its initial agg values copied directly from the first source row
        // except for user-defined COUNTs which get set to 0 or 1 depending on whether the
        // source column value is null.
        for (int aggIndex = 0; aggIndex < m_aggColumnCount; aggIndex++) {
            // set the count(*) column(s) to 1
            if (m_aggTypes[aggIndex] == EXPRESSION_TYPE_AGGREGATE_COUNT_STAR) {
                m_updatedTuple.setNValue((int) (aggOffset+aggIndex), ValueFactory::getBigIntValue(1));
                numCountStar++;
                continue;
            }
            NValue newValue = getAggInputFromSrcTuple(aggIndex, numCountStar, newTuple);
            if (m_aggTypes[aggIndex] == EXPRESSION_TYPE_AGGREGATE_COUNT) {
                if (newValue.isNull()) {
                    newValue = ValueFactory::getBigIntValue(0);
                } else {
                    newValue = ValueFactory::getBigIntValue(1);
                }
            }
            m_updatedTuple.setNValue(aggOffset+aggIndex, newValue);
        }
        // ENG-10892, if no COUNT(*) column exists
        if (numCountStar == 0) {
            // check which hidden column COUNT(*) lies in
            vassert(m_dest->hasViewCountColumn());
            m_updatedTuple.setHiddenNValue(
                    m_dest->getViewCountColumnIndex(), ValueFactory::getBigIntValue(1));
        }
        m_dest->insertPersistentTuple(m_updatedTuple, fallible);
    }
}

void MaterializedViewTriggerForInsert::setDestTable(PersistentTable * dest) {
    PersistentTable* oldDest = m_dest;
    m_dest = dest;
    dest->incrementRefcount();
    dest->setMaterializedViewTrigger(this);

    // Re-initialize dependencies on the dest table, allowing for widened columns
    m_index = m_dest->primaryKeyIndex();

    allocateBackedTuples();

    oldDest->decrementRefcount();
}

void MaterializedViewTriggerForInsert::allocateBackedTuples() {
    uint32_t storeLength;
    char* backingStore;
    // The materialized view will have no index if there is no group by column.
    // In this case, we will not allocate space for m_searchKeyBackingStore (ENG-7872)
    if (m_groupByColumnCount == 0) {
        m_searchKeyBackingStore.reset();
    } else {
        m_searchKeyTuple = TableTuple(m_index->getKeySchema());
        storeLength = m_index->getKeySchema()->tupleLength() + TUPLE_HEADER_SIZE;
        backingStore = new char[storeLength];
        memset(backingStore, 0, storeLength);
        m_searchKeyBackingStore.reset(backingStore);
        m_searchKeyTuple.move(backingStore);
    }

    m_existingTuple = TableTuple(m_dest->schema());

    m_updatedTuple = TableTuple(m_dest->schema());
    storeLength = m_dest->getTupleLength();
    backingStore = new char[storeLength];
    memset(backingStore, 0, storeLength);
    m_updatedTupleBackingStore.reset(backingStore);
    m_updatedTuple.move(backingStore);

    m_emptyTuple = TableTuple(m_dest->schema());
    storeLength = m_dest->getTupleLength();
    backingStore = new char[storeLength];
    memset(backingStore, 0, storeLength);
    m_emptyTupleBackingStore.reset(backingStore);
    m_emptyTuple.move(backingStore);
}

AbstractExpression* MaterializedViewTriggerForInsert::parsePredicate(catalog::MaterializedViewInfo *mvInfo) {
    const string& hexString = mvInfo->predicate();
    if (hexString.size() == 0) {
        return NULL;
    }
    vassert(hexString.length() % 2 == 0);
    int bufferLength = (int)hexString.size() / 2 + 1;
    boost::shared_array<char> buffer(new char[bufferLength]);
    catalog::Catalog::hexDecodeString(hexString, buffer.get());

    PlannerDomRoot domRoot(buffer.get());
    if (domRoot.isNull()) {
        return NULL;
    }
    PlannerDomValue expr = domRoot();
    return AbstractExpression::buildExpressionTree(expr);
}

std::size_t MaterializedViewTriggerForInsert::parseGroupBy(catalog::MaterializedViewInfo *mvInfo) {
    const string& expressionsAsText = mvInfo->groupbyExpressionsJson();
    if (expressionsAsText.length() == 0) {
        // set up the group by columns from the catalog info
        const catalog::CatalogMap<catalog::ColumnRef>& columns = mvInfo->groupbycols();
        m_groupByColIndexes.resize(columns.size());
        for (catalog::CatalogMap<catalog::ColumnRef>::field_map_iter colRefIterator = columns.begin();
             colRefIterator != columns.end(); colRefIterator++) {
            int32_t grouping_order_offset = colRefIterator->second->index();
            m_groupByColIndexes[grouping_order_offset] = colRefIterator->second->column()->index();
        }
        return columns.size();
    }
    VOLT_TRACE("Group by Expression: %s\n", expressionsAsText.c_str());
    ExpressionUtil::loadIndexedExprsFromJson(m_groupByExprs, expressionsAsText);
    return m_groupByExprs.size();
}

std::size_t MaterializedViewTriggerForInsert::parseAggregation(catalog::MaterializedViewInfo *mvInfo) {
    const string& expressionsAsText = mvInfo->aggregationExpressionsJson();
    bool usesComplexAgg = expressionsAsText.length() > 0;
    bool existsCountStar = false;
    // set up the mapping from input col to output col
    const catalog::CatalogMap<catalog::Column>& columns = mvInfo->dest()->columns();
    m_aggTypes.resize(columns.size() - m_groupByColumnCount);
    if ( ! usesComplexAgg) {
        m_aggColIndexes.resize(m_aggTypes.size());
    }
    for (catalog::CatalogMap<catalog::Column>::field_map_iter colIterator = columns.begin();
         colIterator != columns.end(); colIterator++) {
        auto destCol = colIterator->second;
        if (destCol->index() < m_groupByColumnCount) {
            continue;
        }
        // The index into the per-agg metadata starts as a materialized view column index
        // but needs to be shifted down for each column that has no agg option
        // -- that is, -1 for each "group by" AND -1 for the COUNT(*).
        std::size_t aggIndex = destCol->index() - m_groupByColumnCount;
        m_aggTypes[aggIndex] = static_cast<ExpressionType>(destCol->aggregatetype());
        switch(m_aggTypes[aggIndex]) {
            case EXPRESSION_TYPE_AGGREGATE_COUNT_STAR:
                m_countStarColumnIndex = destCol->index();
                existsCountStar = true;
            case EXPRESSION_TYPE_AGGREGATE_SUM:
            case EXPRESSION_TYPE_AGGREGATE_COUNT:
            case EXPRESSION_TYPE_AGGREGATE_MIN:
            case EXPRESSION_TYPE_AGGREGATE_MAX:
                break; // legal value
            default:
                throwSerializableEEException(
                        "Error in materialized view aggregation %d expression type %s",
                        (int)aggIndex, expressionToString(m_aggTypes[aggIndex]).c_str());
        }
        if (usesComplexAgg || (m_aggTypes[aggIndex] == EXPRESSION_TYPE_AGGREGATE_COUNT_STAR) ) {
            continue;
        }
        // Not used for Complex Aggregation case
        const catalog::Column *srcCol = destCol->matviewsource();
        vassert(srcCol);
        m_aggColIndexes[aggIndex] = srcCol->index();
    }

    if (usesComplexAgg) {
        VOLT_TRACE("Aggregate Expression: %s\n", expressionsAsText.c_str());
        ExpressionUtil::loadIndexedExprsFromJson(m_aggExprs, expressionsAsText);
    }

    // user did not assign COUNT(*) explicitly
    if (!existsCountStar) {
        m_countStarColumnIndex = -1;
    }

    return m_aggTypes.size();
}

NValue MaterializedViewTriggerForInsert::getGroupByValueFromSrcTuple(int colIndex, const TableTuple& tuple) {
    if (m_groupByExprs.size() != 0) {
        AbstractExpression* gbExpr = m_groupByExprs[colIndex];
        return gbExpr->eval(&tuple, NULL);
    }

    int gbColIdx = m_groupByColIndexes[colIndex];
    return tuple.getNValue(gbColIdx);
}

void MaterializedViewTriggerForInsert::initializeTupleHavingNoGroupBy(bool fallible) {
    // clear the tuple that will be built to insert or overwrite
    memset(m_updatedTuple.address(), 0, m_dest->getTupleLength());
    int aggOffset = (int)m_groupByColumnCount;
    NValue newValue;
    for (int aggIndex = 0; aggIndex < m_aggColumnCount; aggIndex++) {
        // COUNT(*) column will be zero
        if (m_aggTypes[aggIndex] == EXPRESSION_TYPE_AGGREGATE_COUNT ||
            m_aggTypes[aggIndex] == EXPRESSION_TYPE_AGGREGATE_COUNT_STAR) {
            newValue = ValueFactory::getBigIntValue(0);
        } else {
            newValue = NValue::getNullValue(m_updatedTuple.getSchema()->columnType(aggOffset+aggIndex));
        }
        m_updatedTuple.setNValue(aggOffset+aggIndex, newValue);
    }
    // ENG-10892, if no COUNT(*) exists
    if (m_countStarColumnIndex == -1) {
        vassert(m_dest->hasViewCountColumn());
        m_updatedTuple.setHiddenNValue(m_dest->getViewCountColumnIndex(), ValueFactory::getBigIntValue(0));
    }
    m_dest->insertPersistentTuple(m_updatedTuple, fallible);
}

// Find the existing tuple using a tuple from the delta table.
bool MaterializedViewTriggerForInsert::findExistingTupleUsingDelta(const TableTuple &tuple) {
    // For the case where there is no grouping column, like SELECT COUNT(*) FROM T;
    // We directly return the only row in the view. See ENG-7872.
    if (m_groupByColumnCount == 0) {
        TableIterator iterator = m_dest->iterator();
        iterator.next(m_existingTuple);
        vassert( ! m_existingTuple.isNullTuple());
        return true;
    }

    IndexCursor indexCursor(m_index->getTupleSchema());
    m_index->moveToKeyByTuple(&tuple, indexCursor);

    m_existingTuple = m_index->nextValueAtKey(indexCursor);
    return ! m_existingTuple.isNullTuple();
}

bool MaterializedViewTriggerForInsert::findExistingTuple(const TableTuple &tuple) {
    // For the case where there is no grouping column, like SELECT COUNT(*) FROM T;
    // We directly return the only row in the view. See ENG-7872.
    if (m_groupByColumnCount == 0) {
        TableIterator iterator = m_dest->iterator();
        iterator.next(m_existingTuple);
        vassert( ! m_existingTuple.isNullTuple());
        return true;
    }

    // Assemble a desired view table tuple (only includes the index key columns)
    // based on the information we stored in this trigger.
    for (int colindex = 0; colindex < m_groupByColumnCount; colindex++) {
        NValue value = getGroupByValueFromSrcTuple(colindex, tuple);
        m_searchKeyValue[colindex] = value;
        m_searchKeyTuple.setNValue(colindex, value);
    }

    IndexCursor indexCursor(m_index->getTupleSchema());
    // determine if the row exists (create the empty one if it doesn't)
    m_index->moveToKey(&m_searchKeyTuple, indexCursor);
    m_existingTuple = m_index->nextValueAtKey(indexCursor);
    return ! m_existingTuple.isNullTuple();
}


void MaterializedViewTriggerForStreamInsert::build(StreamedTable *srcTable,
                                                   PersistentTable *destTable,
                                                   catalog::MaterializedViewInfo *mvInfo) {
    VOLT_TRACE("construct MaterializedViewStreamInsertTrigger...");
    MaterializedViewTriggerForStreamInsert* view =
        new MaterializedViewTriggerForStreamInsert(destTable, mvInfo);
    srcTable->addMaterializedView(view);
    VOLT_TRACE("finished initialization.");
}

} // namespace voltdb
