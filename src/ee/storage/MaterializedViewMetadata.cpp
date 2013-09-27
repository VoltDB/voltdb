/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#include <cassert>
#include <cstdio>
#include <vector>
#include "common/types.h"
#include "common/PlannerDomValue.h"
#include "common/FatalException.hpp"
#include "catalog/catalog.h"
#include "catalog/columnref.h"
#include "catalog/column.h"
#include "catalog/table.h"
#include "catalog/materializedviewinfo.h"
#include "expressions/abstractexpression.h"
#include "expressions/tuplevalueexpression.h"
#include "expressions/constantvalueexpression.h"
#include "expressions/comparisonexpression.h"
#include "expressions/expressionutil.h"
#include "indexes/tableindex.h"
#include "storage/persistenttable.h"
#include "storage/MaterializedViewMetadata.h"
#include "boost/foreach.hpp"
#include "boost/shared_array.hpp"

namespace voltdb {

MaterializedViewMetadata::MaterializedViewMetadata(
        PersistentTable *srcTable, PersistentTable *destTable, catalog::MaterializedViewInfo *mvInfo)
        : m_srcTable(srcTable), m_target(destTable), m_filterPredicate(NULL)
{
// DEBUG_STREAM_HERE("New mat view on source table " << srcTable->name() << " @" << srcTable << " view table " << m_target->name() << " @" << m_target);
    // best not to have to worry about the destination table disappearing out from under the source table that feeds it.
    VOLT_TRACE("construct materializedViewMetadata...");

    m_target->incrementRefcount();
    srcTable->addMaterializedView(this);
    // try to load the predicate from the catalog view
    parsePredicate(mvInfo);
    VOLT_TRACE("Start to parse complex group by");
    parseComplexGroupby(mvInfo);
    if (m_groupbyExprs.size() != 0) {
        m_groupByColumnCount = (int32_t)m_groupbyExprs.size();
    } else {
        m_groupByColumnCount = mvInfo->groupbycols().size();
    }

    // set up the group by columns from the catalog info
    m_groupByColumns = new int32_t[m_groupByColumnCount];

    if (m_groupbyExprs.size() == 0) {
        std::map<std::string, catalog::ColumnRef*>::const_iterator colRefIterator;
        for (colRefIterator = mvInfo->groupbycols().begin();
                colRefIterator != mvInfo->groupbycols().end();
                colRefIterator++)
        {
            int32_t grouping_order_offset = colRefIterator->second->index();
            m_groupByColumns[grouping_order_offset] = colRefIterator->second->column()->index();
        }
    }

    parseComplexAggregation(mvInfo);

    // set up the mapping from input col to output col
    m_outputColumnCount = mvInfo->dest()->columns().size();
    m_outputColumnSrcTableIndexes = new int32_t[m_outputColumnCount];
    m_outputColumnAggTypes = new ExpressionType[m_outputColumnCount];
    std::map<std::string, catalog::Column*>::const_iterator colIterator;
    // iterate the source table
    for (colIterator = mvInfo->dest()->columns().begin(); colIterator != mvInfo->dest()->columns().end(); colIterator++) {
        const catalog::Column *destCol = colIterator->second;
        int destIndex = destCol->index();

        m_outputColumnAggTypes[destIndex] = static_cast<ExpressionType>(destCol->aggregatetype());
        const catalog::Column *srcCol = destCol->matviewsource();

        // We set its matview for Complex Aggregation case
        if (srcCol) {
            m_outputColumnSrcTableIndexes[destIndex] = srcCol->index();
        }
        else {
            m_outputColumnSrcTableIndexes[destIndex] = -1;
        }
    }

    m_index = m_target->primaryKeyIndex();

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
// DEBUG_STREAM_HERE("Delete mat view " << m_target->name() << " w/ table @" << m_target);
    freeBackedTuples();
    delete[] m_groupByColumns;
    delete[] m_outputColumnSrcTableIndexes;
    delete[] m_outputColumnAggTypes;
    delete m_filterPredicate;
    for (int ii = 0; ii < m_groupbyExprs.size(); ++ii) {
        delete m_groupbyExprs[ii];
    }
    for (int ii = 0; ii < m_aggregationExprs.size(); ++ii) {
        delete m_aggregationExprs[ii];
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
    m_searchKey = TableTuple(m_index->getKeySchema());
    m_searchKeyBackingStore = new char[m_index->getKeySchema()->tupleLength() + 1];
    memset(m_searchKeyBackingStore, 0, m_index->getKeySchema()->tupleLength() + 1);
    m_searchKey.move(m_searchKeyBackingStore);

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


void MaterializedViewMetadata::parsePredicate(catalog::MaterializedViewInfo *mvInfo) {
    std::string hexString = mvInfo->predicate();
    if (hexString.size() == 0)
        return;

    assert (hexString.length() % 2 == 0);
    int bufferLength = (int)hexString.size() / 2 + 1;
    boost::shared_array<char> buffer(new char[bufferLength]);
    catalog::Catalog::hexDecodeString(hexString, buffer.get());

    PlannerDomRoot domRoot(buffer.get());
    if (!domRoot.isNull()) {
        PlannerDomValue expr = domRoot.rootObject();
        m_filterPredicate = AbstractExpression::buildExpressionTree(expr);
    }
}

void MaterializedViewMetadata::parseComplexGroupby(catalog::MaterializedViewInfo *mvInfo) {
    const std::string expressionsAsText = mvInfo->groupbyExpressionsJson();
    if (expressionsAsText.length() == 0) {
        return;
    }
    VOLT_TRACE("Group by Expression: %s\n", expressionsAsText.c_str());
    ExpressionUtil::loadIndexedExprsFromJson(m_groupbyExprs, expressionsAsText);
}

void MaterializedViewMetadata::parseComplexAggregation(catalog::MaterializedViewInfo *mvInfo) {
    const std::string expressionsAsText = mvInfo->aggregationExpressionsJson();
    if (expressionsAsText.length() == 0) {
        return;
    }
    VOLT_TRACE("Aggregate Expression: %s\n", expressionsAsText.c_str());
    ExpressionUtil::loadIndexedExprsFromJson(m_aggregationExprs, expressionsAsText);
}

void MaterializedViewMetadata::processTupleInsert(TableTuple &newTuple, bool fallible) {
    // don't change the view if this tuple doesn't match the predicate
    if (m_filterPredicate
        && (m_filterPredicate->eval(&newTuple, NULL).isFalse())) {
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

    int colindex = 0;
    // set up the first n columns, based on group-by columns
    for (colindex = 0; colindex < m_groupByColumnCount; colindex++) {
        // note that if the tuple is in the mv's target table,
        // tuple values should be pulled from the existing tuple in
        // that table. This works around a memory ownership issue
        // related to out-of-line strings.
        NValue value;
        if (exists) {
            value = m_existingTuple.getNValue(colindex);
        } else {
            if (m_groupbyExprs.size() != 0) {
                AbstractExpression * expr = m_groupbyExprs.at(colindex);
                value = expr->eval(&newTuple, NULL);
            } else {
                value = newTuple.getNValue(m_groupByColumns[colindex]);
            }
        }
        m_updatedTuple.setNValue(colindex, value);
    }

    // set up the next column, which is a count
    m_updatedTuple.setNValue(colindex,
                             m_existingTuple.getNValue(colindex).op_increment());
    colindex++;

    // set values for the other columns
    for (int i = colindex; i < m_outputColumnCount; i++) {
        NValue newValue;
        if (m_aggregationExprs.size() != 0) {
            AbstractExpression * expr = m_aggregationExprs.at(i-colindex);
            newValue = expr->eval(&newTuple, NULL);
        } else {
            newValue = newTuple.getNValue(m_outputColumnSrcTableIndexes[i]);
        }
        NValue existingValue = m_existingTuple.getNValue(i);

        if (m_outputColumnAggTypes[i] == EXPRESSION_TYPE_AGGREGATE_SUM) {
            m_updatedTuple.setNValue(i, newValue.op_add(existingValue));
        }
        else if (m_outputColumnAggTypes[i] == EXPRESSION_TYPE_AGGREGATE_COUNT) {
            m_updatedTuple.setNValue(i, existingValue.op_increment());
        }
        else if (m_outputColumnAggTypes[i] == EXPRESSION_TYPE_AGGREGATE_MIN) {
            if (exists) {
                if (!newValue.isNull() && newValue.compare(existingValue) < 0) {
                     m_updatedTuple.setNValue(i, newValue);
                } else {
                    m_updatedTuple.setNValue(i, existingValue);
                }
            } else {
                m_updatedTuple.setNValue(i, newValue);
            }
        }
        else if (m_outputColumnAggTypes[i] == EXPRESSION_TYPE_AGGREGATE_MAX) {
            if (exists) {
                if (!newValue.isNull() && newValue.compare(existingValue) > 0) {
                     m_updatedTuple.setNValue(i, newValue);
                } else {
                    m_updatedTuple.setNValue(i, existingValue);
                }
            } else {
                m_updatedTuple.setNValue(i, newValue);
            }
        }
        else {
            char message[128];
            snprintf(message, 128, "Error in materialized view table update for"
                    " col %d. Expression type %d", i, m_outputColumnAggTypes[i]);
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          message);
        }
    }

    // update or insert the row
    if (exists) {
        // Shouldn't need to update group-key-only indexes such as the primary key
        // since their keys shouldn't ever change, but do update other indexes.
        m_target->updateTupleWithSpecificIndexes(m_existingTuple, m_updatedTuple,
                                                 m_updatableIndexList, fallible);
    }
    else {
        m_target->insertPersistentTuple(m_updatedTuple, fallible);
    }
}

void MaterializedViewMetadata::processTupleDelete(TableTuple &oldTuple, bool fallible) {
    // don't change the view if this tuple doesn't match the predicate
    if (m_filterPredicate && (m_filterPredicate->eval(&oldTuple, NULL).isFalse()))
        return;

    // this will assert if the tuple isn't there as param expected is true
    findExistingTuple(oldTuple, true);

    // clear the tuple that will be built to insert or overwrite
    memset(m_updatedTupleBackingStore, 0, m_target->schema()->tupleLength() + 1);

    //printf("  Existing tuple: %s.\n", m_existingTuple.debugNoHeader().c_str());
    //fflush(stdout);

    // set up the first column, which is a count
    NValue count = m_existingTuple.getNValue(m_groupByColumnCount).op_decrement();

    //printf("  Count is: %d.\n", (int)(m_existingTuple.getSlimValue(m_groupByColumnCount).getBigInt()));
    //fflush(stdout);

    // check if we should remove the tuple
    if (count.isZero()) {
        m_target->deleteTuple(m_existingTuple, true);
        return;
    }
    // assume from here that we're just updating the existing row

    int colindex = 0;
    // set up the first n columns, based on group-by columns
    for (colindex = 0; colindex < m_groupByColumnCount; colindex++) {
        // note that if the tuple is in the mv's target table,
        // tuple values should be pulled from the existing tuple in
        // that table. This works around a memory ownership issue
        // related to out-of-line strings.
        m_updatedTuple.setNValue(colindex, m_existingTuple.getNValue(colindex));
    }

    m_updatedTuple.setNValue(colindex, count);
    colindex++;

    // set values for the other columns
    for (int i = colindex; i < m_outputColumnCount; i++) {
        NValue oldValue;
        if (m_aggregationExprs.size() != 0) {
            AbstractExpression * expr = m_aggregationExprs.at(i-colindex);
            oldValue = expr->eval(&oldTuple, NULL);
        } else {
            oldValue = oldTuple.getNValue(m_outputColumnSrcTableIndexes[i]);
        }
        NValue existingValue = m_existingTuple.getNValue(i);

        if (m_outputColumnAggTypes[i] == EXPRESSION_TYPE_AGGREGATE_SUM) {
            m_updatedTuple.setNValue(i, existingValue.op_subtract(oldValue));
        }
        else if (m_outputColumnAggTypes[i] == EXPRESSION_TYPE_AGGREGATE_COUNT) {
            m_updatedTuple.setNValue(i, existingValue.op_decrement());
        }
        else if (m_outputColumnAggTypes[i] == EXPRESSION_TYPE_AGGREGATE_MIN ||
                m_outputColumnAggTypes[i] == EXPRESSION_TYPE_AGGREGATE_MAX) {
            if (existingValue.compare(oldValue) == 0) {
                // re-calculate MIN / MAX
                NValue current, newVal;
                newVal = newVal.castAs(m_target->schema()->columnType(i));
                TableTuple tuple;

                int srcTableColIdx = -1;
                AbstractExpression *aggExpr = NULL;
                if (m_aggregationExprs.size() != 0) {
                    aggExpr = m_aggregationExprs.at(i - colindex);
                } else {
                    srcTableColIdx = m_outputColumnSrcTableIndexes[i];
                }

                // indexscan if an index is available, otherwise tablescan
                if (m_indexForMinMax != NULL) {
                    m_indexForMinMax->moveToKey(&m_searchKey);
                    VOLT_TRACE("Starting to scan tuples using index %s\n", m_indexForMinMax->debug().c_str());
                    while (!(tuple = m_indexForMinMax->nextValueAtKey()).isNullTuple()) {
                        // skip the oldTuple and apply post filter
                        if (tuple.equals(oldTuple) ||
                                (m_filterPredicate && m_filterPredicate->eval(&tuple, NULL).isFalse())) {
                            continue;
                        }
                        VOLT_TRACE("Scanning tuple: %s\n", tuple.debugNoHeader().c_str());
                        if (aggExpr != NULL) {
                            current = aggExpr->eval(&tuple, NULL);
                        } else {
                            current = tuple.getNValue(srcTableColIdx);
                        }
                        if (current.isNull()) {
                            continue;
                        }
                        if (current.compare(existingValue) == 0) {
                            newVal = current;
                            break;
                        }
                        if (newVal.isNull()) {
                            newVal = current;
                        } else {
                            newVal = (m_outputColumnAggTypes[i] == EXPRESSION_TYPE_AGGREGATE_MIN) ?
                                            newVal.op_min(current) : newVal.op_max(current);
                        }
                    }
                } else {
                    // build a filter to find tuples belongs to this group
                    AbstractExpression* filter = NULL;
                    for (int idx = 0; idx < m_groupByColumnCount; idx++) {
                        AbstractExpression * cmpExpr = NULL;
                        AbstractExpression* left = NULL;
                        AbstractExpression* right = NULL;
                        if (m_groupbyExprs.size() != 0) {
                            left = m_groupbyExprs.at(idx);
                            right = new ConstantValueExpression(left->eval(&oldTuple, NULL));
                            cmpExpr = new ComparisonExpression<CmpEq>(EXPRESSION_TYPE_COMPARE_EQUAL, left, right);
                            VOLT_TRACE("left: %s, right: %s, current: %s\n", left->debugInfo("").c_str(), right->debugInfo("").c_str(), cmpExpr->debugInfo("").c_str());
                        } else {
                            left = new TupleValueExpression(0, m_groupByColumns[idx]);
                            right = new ConstantValueExpression(oldTuple.getNValue(m_groupByColumns[idx]));
                            cmpExpr = new ComparisonExpression<CmpEq>(EXPRESSION_TYPE_COMPARE_EQUAL, left, right);
                            VOLT_TRACE("left: %s, right: %s, current: %s\n", left->debugInfo("").c_str(), right->debugInfo("").c_str(), cmpExpr->debugInfo("").c_str());
                        }
                        if (filter == NULL) {
                            filter = cmpExpr;
                        } else {
                            filter = ExpressionUtil::conjunctionFactory(EXPRESSION_TYPE_CONJUNCTION_AND, filter, cmpExpr);
                        }
                    }

                    VOLT_TRACE("Constructed filter: %s\n", filter->debug().c_str());

                    // loop through tuples to find the MIN / MAX
                    bool skippedOne = false;
                    TableTuple scannedTuple(m_srcTable->schema());
                    TableIterator &iterator = m_srcTable->iterator();

                    while (iterator.next(scannedTuple) && filter->eval(&scannedTuple, NULL).isTrue()) {
                        // apply post filter
                        if(m_filterPredicate && m_filterPredicate->eval(&scannedTuple, NULL).isFalse()) {
                            continue;
                        }
                        VOLT_TRACE("Checking tuple: %s\n", scannedTuple.debugNoHeader().c_str());
                        if (aggExpr != NULL) {
                            current = aggExpr->eval(&scannedTuple, NULL);
                        } else {
                            current = scannedTuple.getNValue(srcTableColIdx);
                        }
                        if (current.isNull()) {
                            continue;
                        }
                        if (current.compare(existingValue) == 0) {
                            if (!skippedOne) {
                                VOLT_TRACE("Skip tuple: %s\n", scannedTuple.debugNoHeader().c_str());
                                skippedOne = true;
                                continue;
                            }
                            VOLT_TRACE("Found another tuple with same min / max value, breaking the loop.\n");
                            newVal = current;
                            break;
                        }
                        VOLT_TRACE("\tBefore: current %s, min %s, max %s\n", current.debug().c_str(), min.debug().c_str(), max.debug().c_str());
                        if (newVal.isNull()) {
                            newVal = current;
                        } else {
                            newVal = (m_outputColumnAggTypes[i] == EXPRESSION_TYPE_AGGREGATE_MIN) ?
                                            newVal.op_min(current) : newVal.op_max(current);
                        }
                        VOLT_TRACE("\tAfter: current %s, min %s, max %s\n", current.debug().c_str(), min.debug().c_str(), max.debug().c_str());
                    }
                }
                m_updatedTuple.setNValue(i, newVal);
            } else {
                m_updatedTuple.setNValue(i, existingValue);
            }
        }
        else {
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          "Error in materialized view table"
                                          " update.");
        }
    }

    // update the row
    // Shouldn't need to update group-key-only indexes such as the primary key
    // since their keys shouldn't ever change, but do update other indexes.
    m_target->updateTupleWithSpecificIndexes(m_existingTuple, m_updatedTuple,
                                             m_updatableIndexList, fallible);
}

bool MaterializedViewMetadata::findExistingTuple(TableTuple &oldTuple, bool expected) {
    // find the key for this tuple (which is the group by columns)
    NValue value;
    for (int i = 0; i < m_groupByColumnCount; i++) {
        if (m_groupbyExprs.size() != 0) {
            AbstractExpression * expr = m_groupbyExprs.at(i);
            value = expr->eval(&oldTuple,NULL);
        } else {
            value = oldTuple.getNValue(m_groupByColumns[i]);
        }
        m_searchKey.setNValue(i, value);
    }

    // determine if the row exists (create the empty one if it doesn't)
    m_index->moveToKey(&m_searchKey);
    m_existingTuple = m_index->nextValueAtKey();
    if (m_existingTuple.isNullTuple()) {
        if (expected) {
            std::string name = m_target->name();
            throwFatalException("MaterializedViewMetadata for table %s went"
                                " looking for a tuple in the view and"
                                " expected to find it but didn't", name.c_str());
        }
        return false;
    }
    else {
        return true;
    }
}

} // namespace voltdb
