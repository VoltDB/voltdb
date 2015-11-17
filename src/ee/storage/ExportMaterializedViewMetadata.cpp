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
#include "storage/ExportMaterializedViewMetadata.h"
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
#include "catalog/planfragment.h"
#include "catalog/statement.h"
#include "expressions/abstractexpression.h"
#include "expressions/tuplevalueexpression.h"
#include "expressions/constantvalueexpression.h"
#include "expressions/comparisonexpression.h"
#include "expressions/expressionutil.h"
#include "indexes/tableindex.h"
#include "storage/persistenttable.h"
#include "storage/streamedtable.h"
#include "execution/VoltDBEngine.h"
#include "execution/ExecutorVector.h"
#include "plannodes/indexscannode.h"
#include "executors/abstractexecutor.h"
#include "boost/foreach.hpp"
#include "boost/shared_array.hpp"

ENABLE_BOOST_FOREACH_ON_CONST_MAP(Statement);
typedef std::pair<std::string, catalog::Statement*> LabeledStatement;

using namespace std;
namespace voltdb {

ExportMaterializedViewMetadata::ExportMaterializedViewMetadata(StreamedTable *srcTable,
                                                   PersistentTable *destTable,
                                                   catalog::MaterializedViewInfo *mvInfo)
    : m_srcTable(srcTable), m_target(destTable), m_index(destTable->primaryKeyIndex())
    , m_filterPredicate(parsePredicate(mvInfo))
    , m_groupByColumnCount(parseGroupBy(mvInfo)) // also loads m_groupByExprs/Columns as needed
    , m_searchKeyValue(m_groupByColumnCount)
    , m_minMaxSearchKeyBackingStore(NULL)
    , m_minMaxSearchKeyBackingStoreSize(0)
    , m_aggColumnCount(parseAggregation(mvInfo))
{
    // best not to have to worry about the destination table disappearing out from under the source table that feeds it.
    VOLT_TRACE("construct exportmaterializedViewMetadata...");

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
    // set up fallback query executors for min/max recalculation
    // must be set after indexForMinMax
    setFallbackExecutorVectors(mvInfo->fallbackQueryStmts());

    allocateBackedTuples();

    // Catch up on pre-existing source tuples UNLESS target tuples have already been migrated in.
//    if (m_target->isPersistentTableEmpty()) {
//        TableTuple scannedTuple(srcTable->schema());
//        TableIterator &iterator = srcTable->iterator();
//        while (iterator.next(scannedTuple)) {
//            processTupleInsert(scannedTuple, false);
//        }
//    }
    /* If there is no group by column and the target table is still empty
     * even after catching up with pre-existing source tuples, we should initialize the
     * target table with a row of default values.
     * COUNT() functions should have value 0, other aggregation functions should have value NULL.
     * See ENG-7872
     */
    if (m_groupByColumnCount == 0 && m_target->isPersistentTableEmpty()) {
        initializeTupleHavingNoGroupBy();
    }
    VOLT_TRACE("Finish initialization...");
}

ExportMaterializedViewMetadata::~ExportMaterializedViewMetadata() {
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

void ExportMaterializedViewMetadata::setTargetTable(PersistentTable * target)
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

void ExportMaterializedViewMetadata::setFallbackExecutorVectors(const catalog::CatalogMap<catalog::Statement> &fallbackQueryStmts) {
    m_fallbackExecutorVectors.clear();
    m_usePlanForAgg.clear();
    VoltDBEngine* engine = ExecutorContext::getEngine();
    int idx = 0;
    BOOST_FOREACH (LabeledStatement labeledStatement, fallbackQueryStmts) {
        catalog::Statement *stmt = labeledStatement.second;
        const string& b64plan = stmt->fragments().begin()->second->plannodetree();
        const string jsonPlan = engine->getTopend()->decodeBase64AndDecompress(b64plan);

        boost::shared_ptr<ExecutorVector> execVec = ExecutorVector::fromJsonPlan(engine, jsonPlan, -1);
        // We don't need the send executor.
        execVec->getRidOfSendExecutor();
        m_fallbackExecutorVectors.push_back(execVec);

        /* Decide if we should use the plan or still stick to the hard coded function. -- yzhang
         * For now, we only use the plan to refresh the materialzied view when:
         *     - the generated plan is an index scan plan
         *     AND
         *     - the index that the plan chose is different from the index our hard-coded function chose.
         *       (If the plan uses index scan but our hard-coded function uses sequential scan,
         *        we should also go with the plan.)
         * Things will get different when we add join table materialzied view or CUBE view.
         */
        vector<AbstractExecutor*> executorList = execVec->getExecutorList();
        AbstractPlanNode* apn = executorList[0]->getPlanNode();
        bool usePlanForAgg = false;
        if (apn->getPlanNodeType() == PLAN_NODE_TYPE_INDEXSCAN) {
            TableIndex* hardCodedIndex = m_indexForMinMax[idx];
            if (hardCodedIndex) {
#ifdef VOLT_TRACE_ENABLED
                if (ExecutorContext::getExecutorContext()->m_siteId == 0) {
                    cout << "hard-coded function uses: " << hardCodedIndex->getName() << "\n"
                            << "plan uses: " << ((IndexScanPlanNode *)apn)->getTargetIndexName() << endl;
                }
#endif
                usePlanForAgg = hardCodedIndex->getName().compare( ((IndexScanPlanNode *)apn)->getTargetIndexName() ) != 0;
            }
            else {
                usePlanForAgg = true;
            }
        }
        m_usePlanForAgg.push_back(usePlanForAgg);

#ifdef VOLT_TRACE_ENABLED
        if (ExecutorContext::getExecutorContext()->m_siteId == 0) {
            const string& hexString = stmt->explainplan();
            assert(hexString.length() % 2 == 0);
            int bufferLength = (int)hexString.size() / 2 + 1;
            char* explanation = new char[bufferLength];
            boost::shared_array<char> memoryGuard(explanation);
            catalog::Catalog::hexDecodeString(hexString, explanation);
            cout << "Aggregation " << idx << "\n"
                    << explanation << "\n"
                    << "Uses " << (usePlanForAgg ? "plan.\n" : "hard-coded function.\n") << endl;
        }
#endif
        ++ idx;
    }
}

void ExportMaterializedViewMetadata::setIndexForMinMax(const catalog::CatalogMap<catalog::IndexRef> &indexForMinOrMax)
{
    std::vector<TableIndex*> candidates = m_srcTable->allIndexes();
    m_indexForMinMax.clear();
    for (catalog::CatalogMap<catalog::IndexRef>::field_map_iter idxIterator = indexForMinOrMax.begin();
         idxIterator != indexForMinOrMax.end(); idxIterator++) {
        catalog::IndexRef *idx = idxIterator->second;
        if (idx->name().compare("") == 0) {
            // The min/max column doesn't have a supporting index.
            m_indexForMinMax.push_back(NULL);
        }
        else {
            // The min/max column has a supporting index.
            for (int i = 0; i < candidates.size(); ++i) {
                if (idx->name().compare(candidates[i]->getName()) == 0) {
                    m_indexForMinMax.push_back(candidates[i]);
                    break;
                }
            } // end for
        }
    }
    allocateMinMaxSearchKeyTuple();
}

void ExportMaterializedViewMetadata::freeBackedTuples()
{
    delete[] m_searchKeyBackingStore;
    delete[] m_updatedTupleBackingStore;
    delete[] m_emptyTupleBackingStore;
    delete[] m_minMaxSearchKeyBackingStore;
}

void ExportMaterializedViewMetadata::allocateMinMaxSearchKeyTuple()
{
    size_t minMaxSearchKeyBackingStoreSize = 0;
    BOOST_FOREACH(TableIndex *index, m_indexForMinMax) {
        // Because there might be a lot of indexes, find the largest space they may consume
        // so that they can all share one space and use different schemas. (ENG-8512)
        if ( minMaxIndexIncludesAggCol(index) &&
                index->getKeySchema()->tupleLength() + TUPLE_HEADER_SIZE > minMaxSearchKeyBackingStoreSize) {
             minMaxSearchKeyBackingStoreSize = index->getKeySchema()->tupleLength() + TUPLE_HEADER_SIZE;
        }
    }
    if (minMaxSearchKeyBackingStoreSize == m_minMaxSearchKeyBackingStoreSize) {
        return;
    }
    m_minMaxSearchKeyBackingStoreSize = minMaxSearchKeyBackingStoreSize;
    delete[] m_minMaxSearchKeyBackingStore;
    m_minMaxSearchKeyBackingStore = NULL;
    // If the minMaxIndex contains agg cols, need to allocate a searchKeyTuple and backing store for it. (ENG-6511)
    if ( m_minMaxSearchKeyBackingStoreSize > 0 ) {
        m_minMaxSearchKeyBackingStore = new char[m_minMaxSearchKeyBackingStoreSize];
        memset(m_minMaxSearchKeyBackingStore, 0, m_minMaxSearchKeyBackingStoreSize);
    }
}

void ExportMaterializedViewMetadata::allocateBackedTuples()
{
    // The materialized view will have no index if there is no group by column.
    // In this case, we will not allocate space for m_searchKeyBackingStore (ENG-7872)
    if (m_groupByColumnCount == 0) {
        m_searchKeyBackingStore = NULL;
    }
    else {
        m_searchKeyTuple = TableTuple(m_index->getKeySchema());
        m_searchKeyBackingStore = new char[m_index->getKeySchema()->tupleLength() + TUPLE_HEADER_SIZE];
        memset(m_searchKeyBackingStore, 0, m_index->getKeySchema()->tupleLength() + TUPLE_HEADER_SIZE);
        m_searchKeyTuple.move(m_searchKeyBackingStore);
    }

    m_existingTuple = TableTuple(m_target->schema());

    m_updatedTuple = TableTuple(m_target->schema());
    m_updatedTupleBackingStore = new char[m_target->getTupleLength()];
    memset(m_updatedTupleBackingStore, 0, m_target->getTupleLength());
    m_updatedTuple.move(m_updatedTupleBackingStore);

    m_emptyTuple = TableTuple(m_target->schema());
    m_emptyTupleBackingStore = new char[m_target->getTupleLength()];
    memset(m_emptyTupleBackingStore, 0, m_target->getTupleLength());
    m_emptyTuple.move(m_emptyTupleBackingStore);
}


AbstractExpression* ExportMaterializedViewMetadata::parsePredicate(catalog::MaterializedViewInfo *mvInfo)
{
    const string& hexString = mvInfo->predicate();
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

std::size_t ExportMaterializedViewMetadata::parseGroupBy(catalog::MaterializedViewInfo *mvInfo)
{
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

std::size_t ExportMaterializedViewMetadata::parseAggregation(catalog::MaterializedViewInfo *mvInfo)
{
    const string& expressionsAsText = mvInfo->aggregationExpressionsJson();
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

inline NValue ExportMaterializedViewMetadata::getGroupByValueFromSrcTuple(int colIndex, const TableTuple& tuple)
{
    if (m_groupByExprs.size() != 0) {
        AbstractExpression* gbExpr = m_groupByExprs[colIndex];
        return gbExpr->eval(&tuple, NULL);
    } else {
        int gbColIdx = m_groupByColIndexes[colIndex];
        return tuple.getNValue(gbColIdx);
    }
}

inline NValue ExportMaterializedViewMetadata::getAggInputFromSrcTuple(int aggIndex, const TableTuple& tuple)
{
    if (m_aggExprs.size() != 0) {
        AbstractExpression* aggExpr = m_aggExprs[aggIndex];
        return aggExpr->eval(&tuple, NULL);
    } else {
        int srcColIdx = m_aggColIndexes[aggIndex];
        return tuple.getNValue(srcColIdx);
    }
}

NValue ExportMaterializedViewMetadata::findMinMaxFallbackValueIndexed(const TableTuple& oldTuple,
                                                                const NValue &existingValue,
                                                                const NValue &initialNull,
                                                                int negate_for_min,
                                                                int aggIndex,
                                                                int minMaxAggIdx)
{
    AbstractExpression *aggExpr = NULL;
    int srcColIdx = -1;
    if (m_aggExprs.size() != 0) {
        aggExpr = m_aggExprs[aggIndex];
    } else {
        srcColIdx = m_aggColIndexes[aggIndex];
    }
    NValue newVal = initialNull;
    TableIndex *selectedIndex = m_indexForMinMax[minMaxAggIdx];
    IndexCursor minMaxCursor(selectedIndex->getTupleSchema());

    // Search for the min / max fallback value. use indexs differently according to their types.
    // (Does the index include min / max aggCol? - ENG-6511)
    if ( minMaxIndexIncludesAggCol(selectedIndex) ) {
        // Assemble the m_minMaxSearchKeyTuple with
        // group-by column values and the old min/max value.
        m_minMaxSearchKeyTuple = TableTuple(selectedIndex->getKeySchema());
        m_minMaxSearchKeyTuple.move(m_minMaxSearchKeyBackingStore);
        for (int colindex = 0; colindex < m_groupByColumnCount; colindex++) {
            NValue value = getGroupByValueFromSrcTuple(colindex, oldTuple);
            m_minMaxSearchKeyTuple.setNValue(colindex, value);
        }
        NValue oldValue = getAggInputFromSrcTuple(aggIndex, oldTuple);
        m_minMaxSearchKeyTuple.setNValue((int)m_groupByColumnCount, oldValue);
        TableTuple tuple;
        // Search for the new min/max value and keep it in tuple.
        if (negate_for_min == -1) {
            // min()
            selectedIndex->moveToKeyOrGreater(&m_minMaxSearchKeyTuple, minMaxCursor);
        }
        else {
            // max()
            selectedIndex->moveToGreaterThanKey(&m_minMaxSearchKeyTuple, minMaxCursor);
            selectedIndex->moveToPriorEntry(minMaxCursor);
        }
        while ( ! (tuple = selectedIndex->nextValue(minMaxCursor)).isNullTuple() ) {
            // If the cursor already moved out of the target group range, exit the loop.
            for (int colindex = 0; colindex < m_groupByColumnCount; colindex++) {
                NValue value = getGroupByValueFromSrcTuple(colindex, tuple);
                if ( value.compare(m_searchKeyValue[colindex]) != 0 ) {
                    return initialNull;
                }
            }
            // skip the oldTuple and apply post filter
            if (tuple.address() == oldTuple.address() ||
                (m_filterPredicate && !m_filterPredicate->eval(&tuple, NULL).isTrue())) {
                continue;
            }
            NValue current = (aggExpr) ? aggExpr->eval(&tuple, NULL) : tuple.getNValue(srcColIdx);
            if (current.isNull()) {
                return initialNull;
            }
            newVal = current;
            break;
        }
    }
    else {
        // Use sub-optimal index (only group-by columns).
        selectedIndex->moveToKey(&m_searchKeyTuple, minMaxCursor);
        VOLT_TRACE("Starting to scan tuples using index %s\n", selectedIndex->debug().c_str());
        TableTuple tuple;
        while (!(tuple = selectedIndex->nextValueAtKey(minMaxCursor)).isNullTuple()) {
            // skip the oldTuple and apply post filter
            if (tuple.address() == oldTuple.address() ||
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
    }
    return newVal;
}

NValue ExportMaterializedViewMetadata::findMinMaxFallbackValueSequential(const TableTuple& oldTuple,
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

NValue ExportMaterializedViewMetadata::findFallbackValueUsingPlan(const TableTuple& oldTuple,
                                                            const NValue &initialNull,
                                                            int aggIndex,
                                                            int minMaxAggIdx) {
    // build parameters.
    // the parameters are the groupby columns and the aggregation column.
    ExecutorContext* context = ExecutorContext::getExecutorContext();
    NValueArray &params = *context->getParameterContainer();
    vector<NValue> backups(m_groupByColumnCount+1);
    NValue newVal = initialNull;
    NValue oldValue = getAggInputFromSrcTuple(aggIndex, oldTuple);
    int colindex = 0;
    for (; colindex < m_groupByColumnCount; colindex++) {
        backups[colindex] = params[colindex];
        params[colindex] = getGroupByValueFromSrcTuple(colindex, oldTuple);
    }
    backups[colindex] = params[colindex];
    params[colindex] = oldValue;
    // executing the stored plan.
    vector<AbstractExecutor*> executorList = m_fallbackExecutorVectors[minMaxAggIdx]->getExecutorList();
    Table *tbl = context->executeExecutors(executorList, 0);
    assert(tbl);
    // get the fallback value from the returned table.
    TableIterator iterator = tbl->iterator();
    TableTuple tuple(tbl->schema());
    if (iterator.next(tuple)) {
        newVal = tuple.getNValue(0);
    }
    // For debug:
    // if (context->m_siteId == 0) {
    //     cout << "oldTuple: " << oldTuple.debugNoHeader() << "\n"
    //             << "Return table: " << endl << tbl->debug() << "\n"
    //             << "newValue: " << newValue.debug() << endl;
    // }
    // restore
    for (colindex = 0; colindex <= m_groupByColumnCount; colindex++) {
        params[colindex] = backups[colindex];
    }
    context->cleanupExecutorsForSubquery(executorList);
    return newVal;
}

void ExportMaterializedViewMetadata::initializeTupleHavingNoGroupBy()
{
    // clear the tuple that will be built to insert or overwrite
    memset(m_updatedTupleBackingStore, 0, m_target->getTupleLength());
    // COUNT(*) column will be zero.
    m_updatedTuple.setNValue((int)m_groupByColumnCount, ValueFactory::getBigIntValue(0));
    int aggOffset = (int)m_groupByColumnCount + 1;
    NValue newValue;
    for (int aggIndex = 0; aggIndex < m_aggColumnCount; aggIndex++) {
        if (m_aggTypes[aggIndex] == EXPRESSION_TYPE_AGGREGATE_COUNT) {
            newValue = ValueFactory::getBigIntValue(0);
        }
        else {
            newValue = ValueFactory::getNullValue();
        }
        m_updatedTuple.setNValue(aggOffset+aggIndex, newValue);
    }
    m_target->insertPersistentTuple(m_updatedTuple, true);
}

void ExportMaterializedViewMetadata::processTupleInsert(const TableTuple &newTuple, bool fallible)
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
    memset(m_updatedTupleBackingStore, 0, m_target->getTupleLength());

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

void ExportMaterializedViewMetadata::processTupleDelete(const TableTuple &oldTuple, bool fallible)
{
    // don't change the view if this tuple doesn't match the predicate
    if (m_filterPredicate && !m_filterPredicate->eval(&oldTuple, NULL).isTrue())
        return;

    if ( ! findExistingTuple(oldTuple)) {
        std::string name = m_target->name();
        throwFatalException("ExportMaterializedViewMetadata for table %s went"
                            " looking for a tuple in the view and"
                            " expected to find it but didn't", name.c_str());
    }

    // clear the tuple that will be built to insert or overwrite
    memset(m_updatedTupleBackingStore, 0, m_target->getTupleLength());

    // set up the first column, which is a count
    NValue count = m_existingTuple.getNValue((int)m_groupByColumnCount).op_decrement();

    // check if we should remove the tuple
    if (count.isZero()) {
        m_target->deleteTuple(m_existingTuple, fallible);
        // If there is no group by column, the count() should remain 0 and other functions should
        // have value null. See ENG-7872.
        if (m_groupByColumnCount == 0) {
            initializeTupleHavingNoGroupBy();
        }
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
    int minMaxAggIdx = 0;
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
                    if (m_usePlanForAgg[minMaxAggIdx]) {
                        newValue = findFallbackValueUsingPlan(oldTuple, newValue, aggIndex, minMaxAggIdx);
                    }
                    // indexscan if an index is available, otherwise tablescan
                    else if (m_indexForMinMax[minMaxAggIdx]) {
                        newValue = findMinMaxFallbackValueIndexed(oldTuple, existingValue, newValue,
                                                                  reversedForMin, aggIndex, minMaxAggIdx);
                    }
                    else {
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
        if (m_aggTypes[aggIndex] == EXPRESSION_TYPE_AGGREGATE_MIN ||
            m_aggTypes[aggIndex] == EXPRESSION_TYPE_AGGREGATE_MAX) {
            minMaxAggIdx++;
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

bool ExportMaterializedViewMetadata::findExistingTuple(const TableTuple &tuple)
{
    // For the case where is no grouping column, like SELECT COUNT(*) FROM T;
    // We directly return the only row in the view. See ENG-7872.
    if (m_groupByColumnCount == 0) {
        TableIterator iterator = m_target->iteratorDeletingAsWeGo();
        iterator.next(m_existingTuple);
        return ! m_existingTuple.isNullTuple();
    }

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
