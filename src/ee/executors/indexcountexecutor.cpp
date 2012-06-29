/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "IndexCountExecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"

// Inline PlanNodes
#include "plannodes/indexcountnode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/limitnode.h"

#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"

using namespace voltdb;

bool IndexCountExecutor::p_init(AbstractPlanNode *abstractNode,
                               TempTableLimits* limits)
{
    VOLT_TRACE("init IndexScan Executor");

    m_projectionNode = NULL;
    m_limitNode = NULL;

    m_node = dynamic_cast<IndexCountPlanNode*>(abstractNode);
    assert(m_node);
    assert(m_node->getTargetTable());

    // Create output table based on output schema from the plan
    TupleSchema* schema = m_node->generateTupleSchema(true);
    int column_count = static_cast<int>(m_node->getOutputSchema().size());
    std::string* column_names = new std::string[column_count];
    for (int ctr = 0; ctr < column_count; ctr++)
    {
        column_names[ctr] = m_node->getOutputSchema()[ctr]->getColumnName();
    }
    m_node->setOutputTable(TableFactory::getTempTable(m_node->databaseId(),
                                                      m_node->getTargetTable()->name(),
                                                      schema,
                                                      column_names,
                                                      limits));
    delete[] column_names;

    //
    // INLINE PROJECTION
    //
    if (m_node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION) != NULL)
    {
        m_projectionNode =
            static_cast<ProjectionPlanNode*>
            (m_node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION));

        m_projectionExpressions =
            new AbstractExpression*[m_node->getOutputTable()->columnCount()];

        ::memset(m_projectionExpressions, 0,
                 (sizeof(AbstractExpression*) *
                  m_node->getOutputTable()->columnCount()));

        m_projectionAllTupleArrayPtr = ExpressionUtil::convertIfAllTupleValues(m_projectionNode->getOutputColumnExpressions());

        m_projectionAllTupleArray = m_projectionAllTupleArrayPtr.get();

        m_needsSubstituteProjectPtr =
            boost::shared_array<bool>
            (new bool[m_node->getOutputTable()->columnCount()]);
        m_needsSubstituteProject = m_needsSubstituteProjectPtr.get();

        for (int ctr = 0;
             ctr < m_node->getOutputTable()->columnCount();
             ctr++)
        {
            assert(m_projectionNode->getOutputColumnExpressions()[ctr]);
            m_needsSubstituteProjectPtr[ctr] =
              m_projectionNode->
                getOutputColumnExpressions()[ctr]->hasParameter();
            m_projectionExpressions[ctr] =
              m_projectionNode->getOutputColumnExpressions()[ctr];
        }
    }

    //
    // INLINE LIMIT
    //
    if (m_node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT) != NULL)
    {
        m_limitNode =
            static_cast<LimitPlanNode*>
            (m_node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));
    }

    //
    // Make sure that we have search keys and that they're not null
    //
    m_numOfSearchkeys = (int)m_node->getSearchKeyExpressions().size();
    m_searchKeyBeforeSubstituteArrayPtr =
      boost::shared_array<AbstractExpression*>
        (new AbstractExpression*[m_numOfSearchkeys]);
    m_searchKeyBeforeSubstituteArray = m_searchKeyBeforeSubstituteArrayPtr.get();
    m_needsSubstituteSearchKeyPtr =
        boost::shared_array<bool>(new bool[m_numOfSearchkeys]);
    m_needsSubstituteSearchKey = m_needsSubstituteSearchKeyPtr.get();
    // if (m_numOfSearchkeys == 0)
    // {
    //     VOLT_ERROR("There are no search key expressions for PlanNode '%s'",
    //                m_node->debug().c_str());
    //     return false;
    // }
    for (int ctr = 0; ctr < m_numOfSearchkeys; ctr++)
    {
        if (m_node->getSearchKeyExpressions()[ctr] == NULL)
        {
            VOLT_ERROR("The search key expression at position '%d' is NULL for"
                       " PlanNode '%s'", ctr, m_node->debug().c_str());
            delete [] m_projectionExpressions;
            return false;
        }
        m_needsSubstituteSearchKeyPtr[ctr] =
            m_node->getSearchKeyExpressions()[ctr]->hasParameter();
        m_searchKeyBeforeSubstituteArrayPtr[ctr] =
            m_node->getSearchKeyExpressions()[ctr];
    }

    //
    // Initialize local variables
    //

    //output table should be temptable
    m_outputTable = static_cast<TempTable*>(m_node->getOutputTable());
    //target table should be persistenttable
    m_targetTable = static_cast<PersistentTable*>(m_node->getTargetTable());
    m_numOfColumns = static_cast<int>(m_outputTable->columnCount());

    //
    // Grab the Index from our inner table
    // We'll throw an error if the index is missing
    //
    m_index = m_targetTable->index(m_node->getTargetIndexName());
    m_searchKey = TableTuple(m_index->getKeySchema());
    m_searchKeyBackingStore = new char[m_index->getKeySchema()->tupleLength()];
    m_searchKey.moveNoHeader(m_searchKeyBackingStore);
    if (m_index == NULL)
    {
        VOLT_ERROR("Failed to retreive index '%s' from table '%s' for PlanNode"
                   " '%s'", m_node->getTargetIndexName().c_str(),
                   m_targetTable->name().c_str(), m_node->debug().c_str());
        delete [] m_searchKeyBackingStore;
        delete [] m_projectionExpressions;
        return false;
    }
    VOLT_TRACE("Index key schema: '%s'", m_index->getKeySchema()->debug().c_str());

    m_tuple = TableTuple(m_targetTable->schema());

    if (m_node->getEndExpression() != NULL)
    {
        m_needsSubstituteEndExpression =
            m_node->getEndExpression()->hasParameter();
    }
    if (m_node->getPredicate() != NULL)
    {
        m_needsSubstitutePostExpression =
            m_node->getPredicate()->hasParameter();
    }

    //
    // Miscellanous Information
    //
    m_lookupType = m_node->getLookupType();
    m_sortDirection = m_node->getSortDirection();

    // Need to move GTE to find (x,_) when doing a partial covering search.
    // the planner sometimes lies in this case: index_lookup_type_eq is incorrect.
    // Index_lookup_type_gte is necessary. Make the change here.
    if (m_lookupType == INDEX_LOOKUP_TYPE_EQ &&
        m_searchKey.getSchema()->columnCount() > m_numOfSearchkeys)
    {
        VOLT_TRACE("Setting lookup type to GTE for partial covering key.");
        m_lookupType = INDEX_LOOKUP_TYPE_GTE;
    }

    return true;
}

bool IndexCountExecutor::p_execute(const NValueArray &params)
{
    assert(m_node);
    assert(m_node == dynamic_cast<IndexCountPlanNode*>(m_abstractNode));
    assert(m_outputTable);
    assert(m_outputTable == static_cast<TempTable*>(m_node->getOutputTable()));
    assert(m_targetTable);
    assert(m_targetTable == m_node->getTargetTable());
    VOLT_DEBUG("IndexScan: %s.%s\n", m_targetTable->name().c_str(),
               m_index->getName().c_str());

    int activeNumOfSearchKeys = m_numOfSearchkeys;
    IndexLookupType localLookupType = m_lookupType;
    SortDirectionType localSortDirection = m_sortDirection;

    // INLINE PROJECTION
    // Set params to expression tree via substitute()
    assert(m_numOfColumns == m_outputTable->columnCount());
    if (m_projectionNode != NULL && m_projectionAllTupleArray == NULL)
    {
        for (int ctr = 0; ctr < m_numOfColumns; ctr++)
        {
            assert(m_projectionNode->getOutputColumnExpressions()[ctr]);
            if (m_needsSubstituteProject[ctr])
            {
                m_projectionExpressions[ctr]->substitute(params);
            }
            assert(m_projectionExpressions[ctr]);
        }
    }

    //
    // INLINE LIMIT
    //
    if (m_limitNode != NULL)
    {
        m_limitNode->getLimitAndOffsetByReference(params, m_limitSize, m_limitOffset);
    }


    //
    // SEARCH KEY
    //
    m_searchKey.setAllNulls();
    VOLT_TRACE("Initial (all null) search key: '%s'", m_searchKey.debugNoHeader().c_str());
    for (int ctr = 0; ctr < activeNumOfSearchKeys; ctr++) {
        if (m_needsSubstituteSearchKey[ctr]) {
            m_searchKeyBeforeSubstituteArray[ctr]->substitute(params);
        }
        NValue candidateValue = m_searchKeyBeforeSubstituteArray[ctr]->eval(&m_dummy, NULL);
        try {
            m_searchKey.setNValue(ctr, candidateValue);
        }
        catch (SQLException e) {
            // This next bit of logic handles underflow and overflow while
            // setting up the search keys.
            // e.g. TINYINT > 200 or INT <= 6000000000

            // rethow if not an overflow - currently, it's expected to always be an overflow
            if (e.getSqlState() != SQLException::data_exception_numeric_value_out_of_range) {
                throw e;
            }

            // handle the case where this is a comparison, rather than equality match
            // comparison is the only place where the executor might return matching tuples
            // e.g. TINYINT < 1000 should return all values
            if ((localLookupType != INDEX_LOOKUP_TYPE_EQ) &&
                (ctr == (activeNumOfSearchKeys - 1))) {

                if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                    if ((localLookupType == INDEX_LOOKUP_TYPE_GT) ||
                        (localLookupType == INDEX_LOOKUP_TYPE_GTE)) {

                        // gt or gte when key overflows returns nothing
                        return true;
                    }
                    else {
                        // VoltDB should only support LT or LTE with
                        // empty search keys for order-by without lookup
                        throw e;
                    }
                }
                if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                    if ((localLookupType == INDEX_LOOKUP_TYPE_LT) ||
                        (localLookupType == INDEX_LOOKUP_TYPE_LTE)) {

                        // lt or lte when key underflows returns nothing
                        return true;
                    }
                    else {
                        // don't allow GTE because it breaks null handling
                        localLookupType = INDEX_LOOKUP_TYPE_GT;
                    }
                }

                // if here, means all tuples with the previous searchkey
                // columns need to be scaned. Note, if only one column,
                // then all tuples will be scanned
                activeNumOfSearchKeys--;
                if (localSortDirection == SORT_DIRECTION_TYPE_INVALID) {
                    localSortDirection = SORT_DIRECTION_TYPE_ASC;
                }
            }
            // if a EQ comparison is out of range, then return no tuples
            else {
                return true;
            }
            break;
        }
    }
    assert((activeNumOfSearchKeys == 0) || (m_searchKey.getSchema()->columnCount() > 0));
    VOLT_TRACE("Search key after substitutions: '%s'", m_searchKey.debugNoHeader().c_str());


    //
    // END EXPRESSION
    //
    AbstractExpression* end_expression = m_node->getEndExpression();
    if (end_expression != NULL)
    {
        if (m_needsSubstituteEndExpression) {
            end_expression->substitute(params);
        }
        VOLT_DEBUG("End Expression:\n%s", end_expression->debug(true).c_str());
    }

    //
    // POST EXPRESSION
    //
    AbstractExpression* post_expression = m_node->getPredicate();
    if (post_expression != NULL)
    {
        if (m_needsSubstitutePostExpression) {
            post_expression->substitute(params);
        }
        VOLT_DEBUG("Post Expression:\n%s", post_expression->debug(true).c_str());
    }
    assert (m_index);
    assert (m_index == m_targetTable->index(m_node->getTargetIndexName()));

    int tuples_written = 0;
    int tuples_skipped = 0;     // for offset

    //
    // An index scan has three parts:
    //  (1) Lookup tuples using the search key
    //  (2) For each tuple that comes back, check whether the
    //  end_expression is false.
    //  If it is, then we stop scanning. Otherwise...
    //  (3) Check whether the tuple satisfies the post expression.
    //      If it does, then add it to the output table
    //
    // Use our search key to prime the index iterator
    // Now loop through each tuple given to us by the iterator
    //
    if (activeNumOfSearchKeys > 0)
    {
        VOLT_TRACE("INDEX_LOOKUP_TYPE(%d) m_numSearchkeys(%d) key:%s",
                   localLookupType, activeNumOfSearchKeys, m_searchKey.debugNoHeader().c_str());

        if (localLookupType == INDEX_LOOKUP_TYPE_EQ) {
            m_index->moveToKey(&m_searchKey);
        }
        else if (localLookupType == INDEX_LOOKUP_TYPE_GT) {
            m_index->moveToGreaterThanKey(&m_searchKey);
        }
        else if (localLookupType == INDEX_LOOKUP_TYPE_GTE) {
            m_index->moveToKeyOrGreater(&m_searchKey);
        }
        else {
            return false;
        }
    }

    if (localSortDirection != SORT_DIRECTION_TYPE_INVALID) {
        bool order_by_asc = true;

        if (localSortDirection == SORT_DIRECTION_TYPE_ASC) {
            // nothing now
        }
        else {
            order_by_asc = false;
        }

        if (activeNumOfSearchKeys == 0) {
            m_index->moveToEnd(order_by_asc);
        }
    }
    else if (localSortDirection == SORT_DIRECTION_TYPE_INVALID && activeNumOfSearchKeys == 0) {
        return false;
    }

    //
    // We have to different nextValue() methods for different lookup types
    //
    while ((localLookupType == INDEX_LOOKUP_TYPE_EQ &&
            !(m_tuple = m_index->nextValueAtKey()).isNullTuple()) ||
           ((localLookupType != INDEX_LOOKUP_TYPE_EQ || activeNumOfSearchKeys == 0) &&
            !(m_tuple = m_index->nextValue()).isNullTuple()))
    {
        VOLT_TRACE("LOOPING in indexscan: tuple: '%s'\n", m_tuple.debug("tablename").c_str());
        //
        // First check whether the end_expression is now false
        //
        if (end_expression != NULL &&
            end_expression->eval(&m_tuple, NULL).isFalse())
        {
            VOLT_TRACE("End Expression evaluated to false, stopping scan");
            break;
        }
        //
        // Then apply our post-predicate to do further filtering
        //
        if (post_expression == NULL ||
            post_expression->eval(&m_tuple, NULL).isTrue())
        {
            //
            // INLINE OFFSET
            //
            if (m_limitNode != NULL && tuples_skipped < m_limitOffset)
            {
                tuples_skipped++;
                continue;
            }

            if (m_projectionNode != NULL)
            {
                TableTuple &temp_tuple = m_outputTable->tempTuple();
                if (m_projectionAllTupleArray != NULL)
                {
                    VOLT_TRACE("sweet, all tuples");
                    for (int ctr = m_numOfColumns - 1; ctr >= 0; --ctr)
                    {
                        temp_tuple.setNValue(ctr,
                                             m_tuple.getNValue(m_projectionAllTupleArray[ctr]));
                    }
                }
                else
                {
                    for (int ctr = m_numOfColumns - 1; ctr >= 0; --ctr)
                    {
                        temp_tuple.setNValue(ctr,
                                             m_projectionExpressions[ctr]->eval(&m_tuple, NULL));
                    }
                }
                m_outputTable->insertTupleNonVirtual(temp_tuple);
                tuples_written++;
            }
            else
                //
                // Straight Insert
                //
            {
                //
                // Try to put the tuple into our output table
                //
                m_outputTable->insertTupleNonVirtual(m_tuple);
                tuples_written++;
            }

            //
            // INLINE LIMIT
            //
            if (m_limitNode != NULL && tuples_written >= m_limitSize)
            {
                break;
            }
        }
    }

    VOLT_DEBUG ("Index Scanned :\n %s", m_outputTable->debug().c_str());
    return true;
}

IndexCountExecutor::~IndexCountExecutor() {
    delete [] m_searchKeyBackingStore;
    delete [] m_projectionExpressions;
}
