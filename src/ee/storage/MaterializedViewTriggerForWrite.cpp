/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
#include "MaterializedViewTriggerForWrite.h"

#include "persistenttable.h"

#include "catalog/indexref.h"
#include "catalog/planfragment.h"
#include "catalog/statement.h"
#include "execution/ExecutorVector.h"
#include "executors/abstractexecutor.h"
#include "indexes/tableindex.h"
#include "plannodes/indexscannode.h"

ENABLE_BOOST_FOREACH_ON_CONST_MAP(Statement);
typedef std::pair<std::string, catalog::Statement*> LabeledStatement;

using namespace std;
namespace voltdb {

MaterializedViewTriggerForWrite::MaterializedViewTriggerForWrite(PersistentTable *srcTable,
                                                                 PersistentTable *destTable,
                                                                 catalog::MaterializedViewInfo *mvInfo)
    : MaterializedViewTriggerForInsert(destTable, mvInfo)
    , m_srcPersistentTable(srcTable)
    , m_minMaxSearchKeyBackingStoreSize(0)
{
    // set up mechanisms for min/max recalculation
    setupMinMaxRecalculation(mvInfo->indexForMinMax(), mvInfo->fallbackQueryStmts());

    // Catch up on pre-existing source tuples UNLESS target tuples have already been migrated in.
    if (m_target->isPersistentTableEmpty()) {
        /* If there is no group by column, a special initialization is required.
         * COUNT() functions should have value 0, other aggregation functions should have value NULL.
         * See ENG-7872
         */
        if (m_groupByColumnCount == 0) {
            initializeTupleHavingNoGroupBy();
        }
        if ( ! srcTable->isPersistentTableEmpty()) {
            TableTuple scannedTuple(srcTable->schema());
            TableIterator &iterator = srcTable->iterator();
            while (iterator.next(scannedTuple)) {
                processTupleInsert(scannedTuple, false);
            }
        }
    }
}

void MaterializedViewTriggerForWrite::build(PersistentTable *srcTable,
                                            PersistentTable *destTable,
                                            catalog::MaterializedViewInfo *mvInfo) {
    VOLT_TRACE("construct MaterializedViewTriggerForWrite...");
    MaterializedViewTriggerForWrite* view =
        new MaterializedViewTriggerForWrite(srcTable, destTable, mvInfo);
    srcTable->addMaterializedView(view);
    VOLT_TRACE("finished initialization.");
}

MaterializedViewTriggerForWrite::~MaterializedViewTriggerForWrite() { }

void MaterializedViewTriggerForWrite::setupMinMaxRecalculation(const catalog::CatalogMap<catalog::IndexRef> &indexForMinOrMax,
                                                               const catalog::CatalogMap<catalog::Statement> &fallbackQueryStmts) {
    std::vector<TableIndex*> candidates = m_srcPersistentTable->allIndexes();
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

// See if the index is just built on group by columns or it also includes min/max agg (ENG-6511)
static bool minMaxIndexIncludesAggCol(TableIndex * index, size_t groupByColumnCount) {
    return index && index->getColumnIndices().size() > groupByColumnCount;
}

void MaterializedViewTriggerForWrite::allocateMinMaxSearchKeyTuple() {
    uint32_t nextIndexStoreLength;
    size_t minMaxSearchKeyBackingStoreSize = 0;
    BOOST_FOREACH(TableIndex *index, m_indexForMinMax) {
        // Because there might be a lot of indexes, find the largest space they may consume
        // so that they can all share one space and use different schemas. (ENG-8512)
        if (minMaxIndexIncludesAggCol(index, m_groupByColumnCount)) {
            nextIndexStoreLength = index->getKeySchema()->tupleLength() + TUPLE_HEADER_SIZE;
            if (nextIndexStoreLength > minMaxSearchKeyBackingStoreSize) {
                minMaxSearchKeyBackingStoreSize = nextIndexStoreLength;
            }
        }
    }
    if (minMaxSearchKeyBackingStoreSize == m_minMaxSearchKeyBackingStoreSize) {
        return;
    }
    m_minMaxSearchKeyBackingStoreSize = minMaxSearchKeyBackingStoreSize;
    // If the minMaxIndex contains agg cols, need to allocate a searchKeyTuple
    // and backing store for it. (ENG-6511)
    char* backingStore = NULL;
    if (m_minMaxSearchKeyBackingStoreSize > 0) {
        backingStore = new char[m_minMaxSearchKeyBackingStoreSize];
        memset(backingStore, 0, m_minMaxSearchKeyBackingStoreSize);
    }
    m_minMaxSearchKeyBackingStore.reset(backingStore);
}

NValue MaterializedViewTriggerForWrite::findMinMaxFallbackValueIndexed(const TableTuple& oldTuple,
                                                                       const NValue &existingValue,
                                                                       const NValue &initialNull,
                                                                       int negate_for_min,
                                                                       int aggIndex,
                                                                       int minMaxAggIdx) {
    AbstractExpression *aggExpr = NULL;
    int srcColIdx = -1;
    if (m_aggExprs.size() != 0) {
        aggExpr = m_aggExprs[aggIndex];
    }
    else {
        srcColIdx = m_aggColIndexes[aggIndex];
    }
    NValue newVal = initialNull;
    TableIndex *selectedIndex = m_indexForMinMax[minMaxAggIdx];
    IndexCursor minMaxCursor(selectedIndex->getTupleSchema());

    // Search for the min / max fallback value. use indexs differently according to their types.
    // (Does the index include min / max aggCol? - ENG-6511)
    if (minMaxIndexIncludesAggCol(selectedIndex, m_groupByColumnCount)) {
        // Assemble the m_minMaxSearchKeyTuple with
        // group-by column values and the old min/max value.
        m_minMaxSearchKeyTuple = TableTuple(selectedIndex->getKeySchema());
        m_minMaxSearchKeyTuple.move(m_minMaxSearchKeyBackingStore.get());
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
            if (tuple.address() == oldTuple.address() || failsPredicate(tuple)) {
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
            if (tuple.address() == oldTuple.address() || failsPredicate(tuple)) {
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

NValue MaterializedViewTriggerForWrite::findMinMaxFallbackValueSequential(const TableTuple& oldTuple,
                                                                          const NValue &existingValue,
                                                                          const NValue &initialNull,
                                                                          int negate_for_min,
                                                                          int aggIndex) {
    AbstractExpression *aggExpr = NULL;
    int srcColIdx = -1;
    if (m_aggExprs.size() != 0) {
        aggExpr = m_aggExprs[aggIndex];
    } else {
        srcColIdx = m_aggColIndexes[aggIndex];
    }
    NValue newVal = initialNull;
    // loop through tuples to find the MIN / MAX
    TableTuple tuple(m_srcPersistentTable->schema());
    TableIterator &iterator = m_srcPersistentTable->iterator();
    VOLT_TRACE("Starting iteration on: %s\n", m_srcPersistentTable->debug().c_str());
    while (iterator.next(tuple)) {
        // apply post filter
        VOLT_TRACE("Checking tuple: %s\n", tuple.debugNoHeader().c_str());
        if (failsPredicate(tuple)) {
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

NValue MaterializedViewTriggerForWrite::findFallbackValueUsingPlan(const TableTuple& oldTuple,
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

void MaterializedViewTriggerForWrite::processTupleDelete(const TableTuple &oldTuple,
        bool fallible) {
    // don't change the view if this tuple doesn't match the predicate
    if (failsPredicate(oldTuple)) {
        return;
    }

    if ( ! findExistingTuple(oldTuple)) {
        std::string name = m_target->name();
        throwFatalException("MaterializedViewTriggerForWrite for table %s went"
                            " looking for a tuple in the view and"
                            " expected to find it but didn't", name.c_str());
    }

    // clear the tuple that will be built to insert or overwrite
    memset(m_updatedTuple.address(), 0, m_target->getTupleLength());

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
                // no break
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
                // no break
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

} // namespace voltdb
