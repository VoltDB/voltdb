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
    : m_target(destTable)
    , m_index(destTable->primaryKeyIndex())
    , m_filterPredicate(parsePredicate(mvInfo))
    , m_groupByColumnCount(parseGroupBy(mvInfo)) // also loads m_groupByExprs/Columns as needed
    , m_searchKeyValue(m_groupByColumnCount)
    , m_aggColumnCount(parseAggregation(mvInfo))
{
    VOLT_TRACE("Construct MaterializedViewTriggerForInsert...");

    m_mvInfo = mvInfo;

    // best not to have to worry about the destination table disappearing
    // out from under the source table that feeds it.
    m_target->incrementRefcount();

    // When updateTupleWithSpecificIndexes needs to be called,
    // the context is lost that identifies which base table columns potentially changed.
    // So the minimal set of indexes that MIGHT need to be updated must include
    // any that are not solely based on primary key components.
    // Until the DDL compiler does this analysis and marks the indexes accordingly,
    // include all target table indexes except the actual primary key index on the group by columns.
    initUpdatableIndexList();
    allocateBackedTuples();

    VOLT_TRACE("Finished MaterializedViewTriggerForInsert initialization...");
}

MaterializedViewTriggerForInsert::~MaterializedViewTriggerForInsert() {
    for (int ii = 0; ii < m_groupByExprs.size(); ++ii) {
        delete m_groupByExprs[ii];
    }
    for (int ii = 0; ii < m_aggExprs.size(); ++ii) {
        delete m_aggExprs[ii];
    }
    m_target->decrementRefcount();
}

void MaterializedViewTriggerForInsert::initUpdatableIndexList() {
    // Note that if the way we initialize this m_updatableIndexList changes in the future,
    //   we will also need to change the condition to detect when the m_updatableIndexList
    //   should be refreshed in the updateDefinition() function.
    const std::vector<TableIndex*>& targetIndexes = m_target->allIndexes();
    m_updatableIndexList.clear();
    BOOST_FOREACH(TableIndex *index, targetIndexes) {
        if (index != m_index) {
            m_updatableIndexList.push_back(index);
        }
    }
}

void MaterializedViewTriggerForInsert::updateDefinition(PersistentTable *destTable, catalog::MaterializedViewInfo *mvInfo) {
    setTargetTable(destTable);
    initUpdatableIndexList();
}

NValue MaterializedViewTriggerForInsert::getAggInputFromSrcTuple(int aggIndex,
                                                                 const TableTuple& tuple) {
    if (m_aggExprs.size() != 0) {
        AbstractExpression* aggExpr = m_aggExprs[aggIndex];
        return aggExpr->eval(&tuple, NULL);
    }

    int srcColIdx = m_aggColIndexes[aggIndex];
    return tuple.getNValue(srcColIdx);
}

void MaterializedViewTriggerForInsert::processTupleInsert(const TableTuple &newTuple,
                                                          bool fallible) {
    // don't change the view if this tuple doesn't match the predicate
    if (failsPredicate(newTuple)) {
        return;
    }
    bool exists = findExistingTuple(newTuple);
    if (!exists) {
        // create a blank tuple
        VOLT_TRACE("newTuple does not exist,create a blank tuple");
        m_existingTuple.move(m_emptyTuple.address());
    }

    // clear the tuple that will be built to insert or overwrite
    memset(m_updatedTuple.address(), 0, m_target->getTupleLength());

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
            }
            else {
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
                    // no break
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
                }
                else {
                    newValue = ValueFactory::getBigIntValue(1);
                }
            }
            m_updatedTuple.setNValue(aggOffset+aggIndex, newValue);
        }
        m_target->insertPersistentTuple(m_updatedTuple, fallible);
    }
}

void MaterializedViewTriggerForInsert::setTargetTable(PersistentTable * target) {
    PersistentTable * oldTarget = m_target;
    m_target = target;
    target->incrementRefcount();

    // Re-initialize dependencies on the target table, allowing for widened columns
    m_index = m_target->primaryKeyIndex();

    allocateBackedTuples();

    oldTarget->decrementRefcount();
}

void MaterializedViewTriggerForInsert::allocateBackedTuples() {
    uint32_t storeLength;
    char* backingStore;
    // The materialized view will have no index if there is no group by column.
    // In this case, we will not allocate space for m_searchKeyBackingStore (ENG-7872)
    if (m_groupByColumnCount == 0) {
        m_searchKeyBackingStore.reset();
    }
    else {
        m_searchKeyTuple = TableTuple(m_index->getKeySchema());
        storeLength = m_index->getKeySchema()->tupleLength() + TUPLE_HEADER_SIZE;
        backingStore = new char[storeLength];
        memset(backingStore, 0, storeLength);
        m_searchKeyBackingStore.reset(backingStore);
        m_searchKeyTuple.move(backingStore);
    }

    m_existingTuple = TableTuple(m_target->schema());

    m_updatedTuple = TableTuple(m_target->schema());
    storeLength = m_target->getTupleLength();
    backingStore = new char[storeLength];
    memset(backingStore, 0, storeLength);
    m_updatedTupleBackingStore.reset(backingStore);
    m_updatedTuple.move(backingStore);

    m_emptyTuple = TableTuple(m_target->schema());
    storeLength = m_target->getTupleLength();
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
    memset(m_updatedTuple.address(), 0, m_target->getTupleLength());
    // COUNT(*) column will be zero.
    m_updatedTuple.setNValue((int)m_groupByColumnCount, ValueFactory::getBigIntValue(0));
    int aggOffset = (int)m_groupByColumnCount + 1;
    NValue newValue;
    for (int aggIndex = 0; aggIndex < m_aggColumnCount; aggIndex++) {
        if (m_aggTypes[aggIndex] == EXPRESSION_TYPE_AGGREGATE_COUNT) {
            newValue = ValueFactory::getBigIntValue(0);
        }
        else {
            newValue = NValue::getNullValue(m_updatedTuple.getSchema()->columnType(aggOffset+aggIndex));
        }
        m_updatedTuple.setNValue(aggOffset+aggIndex, newValue);
    }
    m_target->insertPersistentTuple(m_updatedTuple, fallible);
}

bool MaterializedViewTriggerForInsert::findExistingTuple(const TableTuple &tuple) {
    // For the case where there is no grouping column, like SELECT COUNT(*) FROM T;
    // We directly return the only row in the view. See ENG-7872.
    if (m_groupByColumnCount == 0) {
        TableIterator iterator = m_target->iterator();
        iterator.next(m_existingTuple);
        assert( ! m_existingTuple.isNullTuple());
        return true;
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
