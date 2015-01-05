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
#include "storage/MaterializedViewMetadata.h"
#include <cassert>
#include <cstdio>
#include <vector>
#include "common/types.h"
#include "common/PlannerDomValue.h"
#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "catalog/catalog.h"
#include "catalog/columnref.h"
#include "catalog/column.h"
#include "catalog/table.h"
#include "expressions/abstractexpression.h"
#include "expressions/tuplevalueexpression.h"
#include "expressions/constantvalueexpression.h"
#include "expressions/comparisonexpression.h"
#include "expressions/expressionutil.h"
#include "indexes/tableindex.h"
#include "storage/persistenttable.h"
#include "boost/foreach.hpp"
#include "boost/shared_array.hpp"

namespace voltdb {

MaterializedViewMetadata::MaterializedViewMetadata(PersistentTable *srcTable,
                                                   PersistentTable *destTable,
                                                   catalog::MaterializedViewInfo *mvInfo)
    : m_srcTable(srcTable), m_target(destTable), m_index(destTable->primaryKeyIndex())
    , m_indexForMinMax(NULL)
    , m_filterPredicate(parsePredicate(mvInfo))
    , m_groupByColumnCount(parseGroupBy(mvInfo)) // also loads m_groupByExprs/Columns as needed
    , m_searchKeyValue(m_groupByColumnCount)
    , m_aggColumnCount(parseAggregation(mvInfo))
{
    // best not to have to worry about the destination table disappearing out from under the source table that feeds it.
    VOLT_TRACE("construct materializedViewMetadata...");

    m_mvInfo = mvInfo;

    m_target->incrementRefcount();
    srcTable->addMaterializedView(this);

    // When updateTupleWithSpecificIndexes needs to be called,
    // the context is lost that identifies which base table columns potentially changed.
    // So the minimal set of indexes that MIGHT need to be updated must include
    // any that are not solely based on primary key components.
    // Until the DDL compiler does this analysis and marks the indexes accordingly,
    // include all target table indexes except the actual primary key index on the group by columns.
    const std::vector<TableIndex*>& targetIndexes = m_target->allIndexes();
    BOOST_FOREACH(TableIndex *index, targetIndexes) {
        if (index != m_index) {
            m_updatableIndexList.push_back(index);
        }
    }

    // handle index for min / max support
    setIndexForMinMax(mvInfo->indexForMinMax());

    allocateBackedTuples();

    // Catch up on pre-existing source tuples UNLESS target tuples have already been migrated in.
    if (( ! srcTable->isPersistentTableEmpty()) && m_target->isPersistentTableEmpty()) {
        TableTuple scannedTuple(srcTable->schema());
        TableIterator &iterator = srcTable->iterator();
        while (iterator.next(scannedTuple)) {
            processTupleInsert(scannedTuple, false);
        }
    }
    VOLT_TRACE("Finish initialization...");
}

MaterializedViewMetadata::~MaterializedViewMetadata() {
    freeBackedTuples();
    delete m_filterPredicate;
    for (int ii = 0; ii < m_groupByExprs.size(); ++ii) {
        delete m_groupByExprs[ii];
    }
    for (int ii = 0; ii < m_aggExprs.size(); ++ii) {
        delete m_aggExprs[ii];
    }
    m_target->decrementRefcount();
}

void MaterializedViewMetadata::setTargetTable(PersistentTable * target)
{
    PersistentTable * oldTarget = m_target;

    m_target = target;
    target->incrementRefcount();

    // Re-initialize dependencies on the target table, allowing for widened columns
    m_index = m_target->primaryKeyIndex();

    freeBackedTuples();
    allocateBackedTuples();

    oldTarget->decrementRefcount();
}

void MaterializedViewMetadata::setIndexForMinMax(std::string indexForMinOrMax)
{
    m_indexForMinMax = NULL;
    if (indexForMinOrMax.compare("") != 0) {
        std::vector<TableIndex*> candidates = m_srcTable->allIndexes();
        for (int i = 0; i < candidates.size(); i++) {
            if (indexForMinOrMax.compare(candidates[i]->getName()) == 0) {
                m_indexForMinMax = candidates[i];
                break;
            }
        }
    }
}

void MaterializedViewMetadata::freeBackedTuples()
{
    delete[] m_searchKeyBackingStore;
    delete[] m_updatedTupleBackingStore;
    delete[] m_emptyTupleBackingStore;
}

void MaterializedViewMetadata::allocateBackedTuples()
{
    m_searchKeyTuple = TableTuple(m_index->getKeySchema());
    m_searchKeyBackingStore = new char[m_index->getKeySchema()->tupleLength() + 1];
    memset(m_searchKeyBackingStore, 0, m_index->getKeySchema()->tupleLength() + 1);
    m_searchKeyTuple.move(m_searchKeyBackingStore);

    m_existingTuple = TableTuple(m_target->schema());

    m_updatedTuple = TableTuple(m_target->schema());
    m_updatedTupleBackingStore = new char[m_target->schema()->tupleLength() + 1];
    memset(m_updatedTupleBackingStore, 0, m_target->schema()->tupleLength() + 1);
    m_updatedTuple.move(m_updatedTupleBackingStore);

    m_emptyTuple = TableTuple(m_target->schema());
    m_emptyTupleBackingStore = new char[m_target->schema()->tupleLength() + 1];
    memset(m_emptyTupleBackingStore, 0, m_target->schema()->tupleLength() + 1);
    m_emptyTuple.move(m_emptyTupleBackingStore);
}


AbstractExpression* MaterializedViewMetadata::parsePredicate(catalog::MaterializedViewInfo *mvInfo)
{
    std::string hexString = mvInfo->predicate();
    if (hexString.size() == 0) {
        return NULL;
    }
    assert (hexString.length() % 2 == 0);
    int bufferLength = (int)hexString.size() / 2 + 1;
    boost::shared_array<char> buffer(new char[bufferLength]);
    catalog::Catalog::hexDecodeString(hexString, buffer.get());

    PlannerDomRoot domRoot(buffer.get());
    if (domRoot.isNull()) {
        return NULL;
    }
    PlannerDomValue expr = domRoot.rootObject();
    return AbstractExpression::buildExpressionTree(expr);
}

std::size_t MaterializedViewMetadata::parseGroupBy(catalog::MaterializedViewInfo *mvInfo)
{
    const std::string expressionsAsText = mvInfo->groupbyExpressionsJson();
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

std::size_t MaterializedViewMetadata::parseAggregation(catalog::MaterializedViewInfo *mvInfo)
{
    const std::string expressionsAsText = mvInfo->aggregationExpressionsJson();
    bool usesComplexAgg = expressionsAsText.length() > 0;
    // set up the mapping from input col to output col
    const catalog::CatalogMap<catalog::Column>& columns = mvInfo->dest()->columns();
    m_aggTypes.resize(columns.size() - m_groupByColumnCount - 1);
    if ( ! usesComplexAgg) {
        m_aggColIndexes.resize(m_aggTypes.size());
    }
    for (catalog::CatalogMap<catalog::Column>::field_map_iter colIterator = columns.begin();
         colIterator != columns.end(); colIterator++) {
        const catalog::Column *destCol = colIterator->second;
        if (destCol->index() < m_groupByColumnCount + 1) {
            continue;
        }
        // The index into the per-agg metadata starts as a materialized view column index
        // but needs to be shifted down for each column that has no agg option
        // -- that is, -1 for each "group by" AND -1 for the COUNT(*).
        std::size_t aggIndex = destCol->index() - m_groupByColumnCount - 1;
        m_aggTypes[aggIndex] = static_cast<ExpressionType>(destCol->aggregatetype());
        switch(m_aggTypes[aggIndex]) {
        case EXPRESSION_TYPE_AGGREGATE_SUM:
        case EXPRESSION_TYPE_AGGREGATE_COUNT:
        case EXPRESSION_TYPE_AGGREGATE_MIN:
        case EXPRESSION_TYPE_AGGREGATE_MAX:
            break; // legal value
        default: {
            char message[128];
            snprintf(message, 128, "Error in materialized view aggregation %d expression type %s",
                     (int)aggIndex, expressionToString(m_aggTypes[aggIndex]).c_str());
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          message);
        }
        }
        if (usesComplexAgg) {
            continue;
        }
        // Not used for Complex Aggregation case
        const catalog::Column *srcCol = destCol->matviewsource();
        assert(srcCol);
        m_aggColIndexes[aggIndex] = srcCol->index();
    }

    if (usesComplexAgg) {
        VOLT_TRACE("Aggregate Expression: %s\n", expressionsAsText.c_str());
        ExpressionUtil::loadIndexedExprsFromJson(m_aggExprs, expressionsAsText);
    }
    return m_aggTypes.size();
}

inline NValue MaterializedViewMetadata::getGroupByValueFromSrcTuple(int colIndex, const TableTuple& tuple)
{
    if (m_groupByExprs.size() != 0) {
        AbstractExpression* gbExpr = m_groupByExprs[colIndex];
        return gbExpr->eval(&tuple, NULL);
    } else {
        int gbColIdx = m_groupByColIndexes[colIndex];
        return tuple.getNValue(gbColIdx);
    }
}

inline NValue MaterializedViewMetadata::getAggInputFromSrcTuple(int aggIndex, const TableTuple& tuple)
{
    if (m_aggExprs.size() != 0) {
        AbstractExpression* aggExpr = m_aggExprs[aggIndex];
        return aggExpr->eval(&tuple, NULL);
    } else {
        int srcColIdx = m_aggColIndexes[aggIndex];
        return tuple.getNValue(srcColIdx);
    }
}

NValue MaterializedViewMetadata::findMinMaxFallbackValueIndexed(const TableTuple& oldTuple,
                                                                const NValue &existingValue,
                                                                const NValue &initialNull,
                                                                int negate_for_min,
                                                                int aggIndex)
{
    AbstractExpression *aggExpr = NULL;
    int srcColIdx = -1;
    if (m_aggExprs.size() != 0) {
        aggExpr = m_aggExprs[aggIndex];
    } else {
        srcColIdx = m_aggColIndexes[aggIndex];
    }
    NValue newVal = initialNull;
    IndexCursor minMaxCursor(m_indexForMinMax->getTupleSchema());

    m_indexForMinMax->moveToKey(&m_searchKeyTuple, minMaxCursor);
    VOLT_TRACE("Starting to scan tuples using index %s\n", m_indexForMinMax->debug().c_str());
    TableTuple tuple;
    while (!(tuple = m_indexForMinMax->nextValueAtKey(minMaxCursor)).isNullTuple()) {
        // skip the oldTuple and apply post filter
        if (tuple.equals(oldTuple) ||
            (m_filterPredicate && !m_filterPredicate->eval(&tuple, NULL).isTrue())) {
            continue;
        }
        VOLT_TRACE("Scanning tuple: %s\n", tuple.debugNoHeader().c_str());
        NValue current = (aggExpr) ? aggExpr->eval(&tuple, NULL) : tuple.getNValue(srcColIdx);
        if (current.isNull()) {
            continue;
        }
        if (current.compare(existingValue) == 0) {
            newVal = current;
            VOLT_TRACE("Found another tuple with same min / max value, breaking the loop.\n");
            break;
        }
        VOLT_TRACE("\tBefore: current %s, best %s\n", current.debug().c_str(), newVal.debug().c_str());
        if (newVal.isNull() || (negate_for_min * current.compare(newVal)) > 0) {
            newVal = current;
            VOLT_TRACE("\tAfter: new best %s\n", newVal.debug().c_str());
        }
    }
    return newVal;
}

NValue MaterializedViewMetadata::findMinMaxFallbackValueSequential(const TableTuple& oldTuple,
                                                                   const NValue &existingValue,
                                                                   const NValue &initialNull,
                                                                   int negate_for_min,
                                                                   int aggIndex)
{
    AbstractExpression *aggExpr = NULL;
    int srcColIdx = -1;
    if (m_aggExprs.size() != 0) {
        aggExpr = m_aggExprs[aggIndex];
    } else {
        srcColIdx = m_aggColIndexes[aggIndex];
    }
    NValue newVal = initialNull;
    // loop through tuples to find the MIN / MAX
    bool skippedOne = false;
    TableTuple tuple(m_srcTable->schema());
    TableIterator &iterator = m_srcTable->iterator();
    VOLT_TRACE("Starting iteration on: %s\n", m_srcTable->debug().c_str());
    while (iterator.next(tuple)) {
        // apply post filter
        VOLT_TRACE("Checking tuple: %s\n", tuple.debugNoHeader().c_str());
        if (m_filterPredicate && !m_filterPredicate->eval(&tuple, NULL).isTrue()) {
            continue;
        }
        VOLT_TRACE("passed 1\n");
        int comparison = 0;
        for (int idx = 0; idx < m_groupByColumnCount; idx++) {
            NValue foundKey = getGroupByValueFromSrcTuple(idx, tuple);
            comparison = m_searchKeyValue[idx].compare(foundKey);
            if (comparison != 0) {
                break;
            }
        }
        if (comparison != 0) {
            continue;
        }
        VOLT_TRACE("passed 2\n");
        NValue current = (aggExpr) ? aggExpr->eval(&tuple, NULL) : tuple.getNValue(srcColIdx);
        if (current.isNull()) {
            continue;
        }
        if (current.compare(existingValue) == 0) {
            if (!skippedOne) {
                VOLT_TRACE("Skip tuple: %s\n", tuple.debugNoHeader().c_str());
                skippedOne = true;
                continue;
            }
            VOLT_TRACE("Found another tuple with same min / max value, breaking the loop.\n");
            newVal = current;
            break;
        }
        VOLT_TRACE("\tBefore: current %s, best %s\n", current.debug().c_str(), newVal.debug().c_str());
        if (newVal.isNull() || (negate_for_min * current.compare(newVal)) > 0) {
            newVal = current;
            VOLT_TRACE("\tAfter: new best %s\n", newVal.debug().c_str());
        }
    }
    VOLT_TRACE("\tFinal: new best %s\n", newVal.debug().c_str());
    return newVal;
}

void MaterializedViewMetadata::processTupleInsert(const TableTuple &newTuple, bool fallible)
{
    // don't change the view if this tuple doesn't match the predicate
    if (m_filterPredicate && !m_filterPredicate->eval(&newTuple, NULL).isTrue()) {
        return;
    }
    bool exists = findExistingTuple(newTuple);
    if (!exists) {
        // create a blank tuple
        VOLT_TRACE("newTuple does not exist,create a blank tuple");
        m_existingTuple.move(m_emptyTupleBackingStore);
    }

    // clear the tuple that will be built to insert or overwrite
    memset(m_updatedTupleBackingStore, 0, m_target->schema()->tupleLength() + 1);

    // set up the first n columns, based on group-by columns
    for (int colindex = 0; colindex < m_groupByColumnCount; colindex++) {
        // note that if the tuple is in the mv's target table,
        // tuple values should be pulled from the existing tuple in
        // that table. This works around a memory ownership issue
        // related to out-of-line strings.
        NValue value = exists ? m_existingTuple.getNValue(colindex) : m_searchKeyValue[colindex];
        m_updatedTuple.setNValue(colindex, value);
    }

    int aggOffset = (int)m_groupByColumnCount + 1;
    // set values for the other columns
    // update or insert the row
    if (exists) {
        // increment the next column, which is a count(*)
        m_updatedTuple.setNValue((int)m_groupByColumnCount,
                                 m_existingTuple.getNValue((int)m_groupByColumnCount).op_increment());

        for (int aggIndex = 0; aggIndex < m_aggColumnCount; aggIndex++) {
            NValue existingValue = m_existingTuple.getNValue(aggOffset+aggIndex);
            NValue newValue = getAggInputFromSrcTuple(aggIndex, newTuple);
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
                    assert(false); // Should have been caught when the matview was loaded.
                    /* no break */
                }
            }
            m_updatedTuple.setNValue(aggOffset+aggIndex, newValue);
        }

        // Shouldn't need to update group-key-only indexes such as the primary key
        // since their keys shouldn't ever change, but do update other indexes.
        m_target->updateTupleWithSpecificIndexes(m_existingTuple, m_updatedTuple,
                                                 m_updatableIndexList, fallible);
    }
    else {
        // set the next column, which is a count(*), to 1
        m_updatedTuple.setNValue((int)m_groupByColumnCount, ValueFactory::getBigIntValue(1));

        // A new group row gets its initial agg values copied directly from the first source row
        // except for user-defined COUNTs which get set to 0 or 1 depending on whether the
        // source column value is null.
        for (int aggIndex = 0; aggIndex < m_aggColumnCount; aggIndex++) {
            NValue newValue = getAggInputFromSrcTuple(aggIndex, newTuple);
            if (m_aggTypes[aggIndex] == EXPRESSION_TYPE_AGGREGATE_COUNT) {
                if (newValue.isNull()) {
                    newValue = ValueFactory::getBigIntValue(0);
                } else {
                    newValue = ValueFactory::getBigIntValue(1);
                }
            }
            m_updatedTuple.setNValue(aggOffset+aggIndex, newValue);
        }
        m_target->insertPersistentTuple(m_updatedTuple, fallible);
    }
}

void MaterializedViewMetadata::processTupleDelete(const TableTuple &oldTuple, bool fallible)
{
    // don't change the view if this tuple doesn't match the predicate
    if (m_filterPredicate && !m_filterPredicate->eval(&oldTuple, NULL).isTrue())
        return;

    if ( ! findExistingTuple(oldTuple)) {
        std::string name = m_target->name();
        throwFatalException("MaterializedViewMetadata for table %s went"
                            " looking for a tuple in the view and"
                            " expected to find it but didn't", name.c_str());
    }

    // clear the tuple that will be built to insert or overwrite
    memset(m_updatedTupleBackingStore, 0, m_target->schema()->tupleLength() + 1);

    // set up the first column, which is a count
    NValue count = m_existingTuple.getNValue((int)m_groupByColumnCount).op_decrement();

    // check if we should remove the tuple
    if (count.isZero()) {
        m_target->deleteTuple(m_existingTuple, fallible);
        return;
    }
    // assume from here that we're just updating the existing row

    // set up the first n columns, based on group-by columns
    for (int colindex = 0; colindex < m_groupByColumnCount; colindex++) {
        // note that if the tuple is in the mv's target table,
        // tuple values should be pulled from the existing tuple in
        // that table. This works around a memory ownership issue
        // related to out-of-line strings.
        m_updatedTuple.setNValue(colindex, m_existingTuple.getNValue(colindex));
    }

    m_updatedTuple.setNValue((int)m_groupByColumnCount, count);

    int aggOffset = (int)m_groupByColumnCount + 1;
    // set values for the other columns
    for (int aggIndex = 0; aggIndex < m_aggColumnCount; aggIndex++) {
        NValue existingValue = m_existingTuple.getNValue(aggOffset+aggIndex);
        NValue oldValue = getAggInputFromSrcTuple(aggIndex, oldTuple);
        NValue newValue = existingValue;
        if ( ! oldValue.isNull()) {
            int reversedForMin = 1; // initially assume that agg is not MIN.
            switch(m_aggTypes[aggIndex]) {
            case EXPRESSION_TYPE_AGGREGATE_SUM:
                newValue = existingValue.op_subtract(oldValue);
                break;
            case EXPRESSION_TYPE_AGGREGATE_COUNT:
                newValue = oldValue.isNull() ? existingValue : existingValue.op_decrement();
                break;
            case EXPRESSION_TYPE_AGGREGATE_MIN:
                reversedForMin = -1; // fall through...
                /* no break */
            case EXPRESSION_TYPE_AGGREGATE_MAX:
                if (oldValue.compare(existingValue) == 0) {
                    // re-calculate MIN / MAX
                    newValue = NValue::getNullValue(m_target->schema()->columnType(aggOffset+aggIndex));

                    // indexscan if an index is available, otherwise tablescan
                    if (m_indexForMinMax) {
                        newValue = findMinMaxFallbackValueIndexed(oldTuple, existingValue, newValue,
                                                                  reversedForMin, aggIndex);
                    } else {
                        VOLT_TRACE("before findMinMaxFallbackValueSequential\n");
                        newValue = findMinMaxFallbackValueSequential(oldTuple, existingValue, newValue,
                                                                     reversedForMin, aggIndex);
                        VOLT_TRACE("after findMinMaxFallbackValueSequential\n");
                    }
                }
                break;
            default:
                assert(false); // Should have been caught when the matview was loaded.
                /* no break */
            }
        }
        VOLT_TRACE("updating matview tuple column %d\n", (int)(aggOffset+aggIndex));
        m_updatedTuple.setNValue(aggOffset+aggIndex, newValue);
    }

    // update the row
    // Shouldn't need to update group-key-only indexes such as the primary key
    // since their keys shouldn't ever change, but do update other indexes.
    m_target->updateTupleWithSpecificIndexes(m_existingTuple, m_updatedTuple,
                                             m_updatableIndexList, fallible);
}

bool MaterializedViewMetadata::findExistingTuple(const TableTuple &tuple)
{
    // find the key for this tuple (which is the group by columns)
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

} // namespace voltdb
