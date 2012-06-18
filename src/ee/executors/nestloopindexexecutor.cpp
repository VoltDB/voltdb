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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <vector>
#include <string>
#include <stack>
#include "nestloopindexexecutor.h"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "execution/VoltDBEngine.h"
#include "executors/indexscanexecutor.h"
#include "expressions/abstractexpression.h"
#include "expressions/tuplevalueexpression.h"
#include "plannodes/nestloopindexnode.h"
#include "plannodes/indexscannode.h"
#include "storage/table.h"
#include "storage/persistenttable.h"
#include "storage/temptable.h"
#include "indexes/tableindex.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"

using namespace std;
using namespace voltdb;

namespace
{
    // FUTURE: the planner should be able to make this decision and
    // add that info to TupleValueExpression rather than having to
    // play the name game here.  These two methods are currently duped
    // in nestloopexecutor because (a) there wasn't an obvious
    // common locale to put them and (b) I hope to make them go away
    // soon.
    bool
    assignTupleValueIndex(AbstractExpression *ae,
                          const std::string &oname,
                          const std::string &iname)
    {
        VOLT_TRACE("assignTupleValueIndex with tables:\n outer: %s, inner %s", oname.c_str(), iname.c_str());

        // if an exact table name match is found, do the obvious
        // thing. Otherwise, assign to the table named "temp".
        // If both tables are named temp, barf; planner purports
        // not accept joins of two temp tables.

        // tuple index 0 is always the outer table.
        // tuple index 1 is always the inner table.
        TupleValueExpression *tve = dynamic_cast<TupleValueExpression*>(ae);
        std::string tname = tve->getTableName();

        if (oname == "temp" && iname == "temp") {
            VOLT_ERROR("Unsuported join on two temp tables.");
            return false;
        }

        if (tname == oname)
            tve->setTupleIndex(0);
        else if (tname == iname)
            tve->setTupleIndex(1);
        else if (oname == "temp")
            tve->setTupleIndex(0);
        else if (iname == "temp")
            tve->setTupleIndex(1);
        else {
            VOLT_ERROR("TableTupleValue in join with unknown table name:\n outer: %s, inner %s", oname.c_str(), iname.c_str());
            return false;
        }

        return true;
    }

    bool
    assignTupleValueIndexes(AbstractExpression* expression,
                            const string& outer_name,
                            const string& inner_name)
    {
        // for each tuple value expression in the expression, determine
        // which tuple is being represented. Tuple could come from outer
        // table or inner table. Configure the predicate to use the correct
        // eval() tuple parameter. By convention, eval's first parameter
        // will always be the outer table and its second parameter the inner
        const AbstractExpression* predicate = expression;
        std::stack<const AbstractExpression*> stack;
        while (predicate != NULL) {
            const AbstractExpression *left = predicate->getLeft();
            const AbstractExpression *right = predicate->getRight();

            if (right != NULL) {
                if (right->getExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE) {
                    if (!assignTupleValueIndex(const_cast<AbstractExpression*>(right),
                                               outer_name,
                                               inner_name))
                    {
                        return false;
                    }
                }
                // remember the right node - must visit its children
                stack.push(right);
            }
            if (left != NULL) {
                if (left->getExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE) {
                    if (!assignTupleValueIndex(const_cast<AbstractExpression*>(left),
                                               outer_name,
                                               inner_name))
                    {
                        return false;
                    }
                }
            }

            predicate = left;
            if (!predicate && !stack.empty()) {
                predicate = stack.top();
                stack.pop();
            }
        }
        return true;
    }
}

bool NestLoopIndexExecutor::p_init(AbstractPlanNode* abstractNode,
                                   TempTableLimits* limits)
{
    VOLT_TRACE("init NLIJ Executor");
    assert(limits);

    node = dynamic_cast<NestLoopIndexPlanNode*>(abstractNode);
    assert(node);
    inline_node = dynamic_cast<IndexScanPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_INDEXSCAN));
    assert(inline_node);
    join_type = node->getJoinType();
    m_lookupType = inline_node->getLookupType();
    m_sortDirection = inline_node->getSortDirection();

    //
    // We need exactly one input table and a target table
    //
    assert(node->getInputTables().size() == 1);

    int schema_size = static_cast<int>(node->getOutputSchema().size());
    string* columnNames = new string[schema_size];
    for (int i = 0; i < schema_size; i++)
    {
        columnNames[i] = node->getOutputSchema()[i]->getColumnName();
        m_outputExpressions.
            push_back(node->getOutputSchema()[i]->getExpression());
    }

    TupleSchema* schema = node->generateTupleSchema(true);

    // create the output table
    node->setOutputTable(
        TableFactory::getTempTable(node->getInputTables()[0]->databaseId(),
                                   "temp", schema, columnNames,
                                   limits));
    delete[] columnNames;

    //
    // Make sure that we actually have search keys
    //
    int num_of_searchkeys = (int)inline_node->getSearchKeyExpressions().size();
    //nshi commented this out in revision 4495 of the old repo in index scan executor
    //the code is cut and paste in nest loop and the change is necessary here as well
//    if (num_of_searchkeys == 0) {
//        VOLT_ERROR("There are no search key expressions for the internal"
//                   " PlanNode '%s' of PlanNode '%s'",
//                   inline_node->debug().c_str(), node->debug().c_str());
//        return false;
//    }
    for (int ctr = 0; ctr < num_of_searchkeys; ctr++) {
        if (inline_node->getSearchKeyExpressions()[ctr] == NULL) {
            VOLT_ERROR("The search key expression at position '%d' is NULL for"
                       " internal PlanNode '%s' of PlanNode '%s'",
                       ctr, inline_node->debug().c_str(), node->debug().c_str());
            return false;
        }
    }

    // output must be a temp table
    output_table = dynamic_cast<TempTable*>(node->getOutputTable());
    assert(output_table);

    inner_table = dynamic_cast<PersistentTable*>(inline_node->getTargetTable());
    assert(inner_table);

    assert(node->getInputTables().size() == 1);
    outer_table = node->getInputTables()[0];
    assert(outer_table);

    //
    // Grab the Index from our inner table
    // We'll throw an error if the index is missing
    //
    index = inner_table->index(inline_node->getTargetIndexName());
    if (index == NULL) {
        VOLT_ERROR("Failed to retreive index '%s' from inner table '%s' for"
                   " internal PlanNode '%s'",
                   inline_node->getTargetIndexName().c_str(),
                   inner_table->name().c_str(), inline_node->debug().c_str());
        return false;
    }

    index_values = TableTuple(index->getKeySchema());
    index_values_backing_store = new char[index->getKeySchema()->tupleLength()];
    index_values.move( index_values_backing_store - TUPLE_HEADER_SIZE);
    index_values.setAllNulls();

    // for each tuple value expression in the predicate, determine
    // which tuple is being represented. Tuple could come from outer
    // table or inner table. Configure the predicate to use the correct
    // eval() tuple parameter. By convention, eval's first parameter
    // will always be the outer table and its second parameter the inner

    // NOTE: the output expressions are not currently scanned to
    // determine how to take tuples from the outer and inner tables,
    // since the way the execute loop is currently written prevents
    // the contribution to the schema from the outer table from being
    // used with a valid tuple from the inner table.

    bool retval =
        assignTupleValueIndexes(inline_node->getPredicate(),
                                node->getInputTables()[0]->name(),
                                inline_node->getTargetTable()->name());

    retval &=
        assignTupleValueIndexes(inline_node->getEndExpression(),
                                node->getInputTables()[0]->name(),
                                inline_node->getTargetTable()->name());

    return retval;
}

bool NestLoopIndexExecutor::p_execute(const NValueArray &params)
{
    assert (node == dynamic_cast<NestLoopIndexPlanNode*>(getPlanNode()));
    assert(node);
    assert (inline_node == dynamic_cast<IndexScanPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_INDEXSCAN)));
    assert(inline_node);

    assert (output_table == dynamic_cast<TempTable*>(node->getOutputTable()));
    assert(output_table);

    //inner_table is the table that has the index to be used in this executor
    assert (inner_table == dynamic_cast<PersistentTable*>(inline_node->getTargetTable()));
    assert(inner_table);

    //outer_table is the input table that have tuples to be iterated
    assert(node->getInputTables().size() == 1);
    assert (outer_table == node->getInputTables()[0]);
    assert (outer_table);
    VOLT_TRACE("executing NestLoopIndex with outer table: %s, inner table: %s",
               outer_table->debug().c_str(), inner_table->debug().c_str());

    //
    // Substitute parameter to SEARCH KEY Note that the expressions
    // will include TupleValueExpression even after this substitution
    //
    int num_of_searchkeys = (int)inline_node->getSearchKeyExpressions().size();
    for (int ctr = 0; ctr < num_of_searchkeys; ctr++) {
        VOLT_TRACE("Search Key[%d] before substitution:\n%s",
                   ctr, inline_node->getSearchKeyExpressions()[ctr]->debug(true).c_str());

        inline_node->getSearchKeyExpressions()[ctr]->substitute(params);

        VOLT_TRACE("Search Key[%d] after substitution:\n%s",
                   ctr, inline_node->getSearchKeyExpressions()[ctr]->debug(true).c_str());
    }


    // end expression
    AbstractExpression* end_expression = inline_node->getEndExpression();
    if (end_expression) {
        end_expression->substitute(params);
        VOLT_TRACE("End Expression:\n%s", end_expression->debug(true).c_str());
    }

    // post expression
    AbstractExpression* post_expression = inline_node->getPredicate();
    if (post_expression != NULL) {
        post_expression->substitute(params);
        VOLT_TRACE("Post Expression:\n%s", post_expression->debug(true).c_str());
    }

    //
    // OUTER TABLE ITERATION
    //
    TableTuple outer_tuple(outer_table->schema());
    TableTuple inner_tuple(inner_table->schema());
    TableIterator outer_iterator = outer_table->iterator();
    int num_of_outer_cols = outer_table->columnCount();
    int num_of_inner_cols = inner_table->columnCount();
    assert (outer_tuple.sizeInValues() == outer_table->columnCount());
    assert (inner_tuple.sizeInValues() == inner_table->columnCount());
    TableTuple &join_tuple = output_table->tempTuple();

    while (outer_iterator.next(outer_tuple)) {
        VOLT_TRACE("outer_tuple:%s",
                   outer_tuple.debug(outer_table->name()).c_str());

        int activeNumOfSearchKeys = num_of_searchkeys;
        IndexLookupType localLookupType = m_lookupType;
        SortDirectionType localSortDirection = m_sortDirection;

        // did this loop body find at least one match for this tuple?
        bool match = false;
        // did setting the search key fail (usually due to overflow)
        bool keyException = false;

        //
        // Now use the outer table tuple to construct the search key
        // against the inner table
        //
        index_values.setAllNulls();
        for (int ctr = 0; ctr < activeNumOfSearchKeys; ctr++) {
            // in a normal index scan, params would be substituted here,
            // but this scan fills in params outside the loop
            NValue candidateValue = inline_node->getSearchKeyExpressions()[ctr]->eval(&outer_tuple, NULL);
            try {
                index_values.setNValue(ctr, candidateValue);
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

                    // sanity check that there is at least one EQ column
                    // or else the join wouldn't work, right?
                    assert(activeNumOfSearchKeys > 1);

                    if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                        if ((localLookupType == INDEX_LOOKUP_TYPE_GT) ||
                            (localLookupType == INDEX_LOOKUP_TYPE_GTE)) {

                            // gt or gte when key overflows breaks out
                            // and only returns for left-outer
                            keyException = true;
                            break; // the outer while loop
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

                            // VoltDB should only support LT or LTE with
                            // empty search keys for order-by without lookup
                            throw e;
                        }
                        else {
                            // don't allow GTE because it breaks null handling
                            localLookupType = INDEX_LOOKUP_TYPE_GT;
                        }
                    }

                    // if here, means all tuples with the previous searchkey
                    // columns need to be scaned.
                    activeNumOfSearchKeys--;
                    if (localSortDirection == SORT_DIRECTION_TYPE_INVALID) {
                        localSortDirection = SORT_DIRECTION_TYPE_ASC;
                    }
                }
                // if a EQ comparison is out of range, then the tuple from
                // the outer loop returns no matches (except left-outer)
                else {
                    keyException = true;
                }
                break;
            }
        }
        VOLT_TRACE("Searching %s", index_values.debug("").c_str());


        // if a search value didn't fit into the targeted index key, skip this key
        if (!keyException) {

            //
            // Our index scan on the inner table is going to have three parts:
            //  (1) Lookup tuples using the search key
            //
            //  (2) For each tuple that comes back, check whether the
            //      end_expression is false.  If it is, then we stop
            //      scanning. Otherwise...
            //
            //  (3) Check whether the tuple satisfies the post expression.
            //      If it does, then add it to the output table
            //
            // Use our search key to prime the index iterator
            // The loop through each tuple given to us by the iterator
            //
            // Essentially cut and pasted this if ladder from
            // index scan executor
            if (num_of_searchkeys > 0)
            {
                if (localLookupType == INDEX_LOOKUP_TYPE_EQ) {
                    index->moveToKey(&index_values);
                }
                else if (localLookupType == INDEX_LOOKUP_TYPE_GT) {
                    index->moveToGreaterThanKey(&index_values);
                }
                else if (localLookupType == INDEX_LOOKUP_TYPE_GTE) {
                    index->moveToKeyOrGreater(&index_values);
                }
                else {
                    return false;
                }
            }

            if (m_sortDirection != SORT_DIRECTION_TYPE_INVALID) {
                bool order_by_asc = true;

                if (m_sortDirection == SORT_DIRECTION_TYPE_ASC) {
                    // nothing now
                }
                else {
                    order_by_asc = false;
                }

                if (num_of_searchkeys == 0) {
                    index->moveToEnd(order_by_asc);
                }
            }
            else if (m_sortDirection == SORT_DIRECTION_TYPE_INVALID && num_of_searchkeys == 0) {
                return false;
            }

            while ((localLookupType == INDEX_LOOKUP_TYPE_EQ &&
                    !(inner_tuple = index->nextValueAtKey()).isNullTuple()) ||
                   ((localLookupType != INDEX_LOOKUP_TYPE_EQ || num_of_searchkeys == 0) &&
                    !(inner_tuple = index->nextValue()).isNullTuple()))
            {
                match = true;
                VOLT_TRACE("inner_tuple:%s",
                           inner_tuple.debug(inner_table->name()).c_str());

                //
                // First check whether the end_expression is now false
                //
                if (end_expression != NULL &&
                    end_expression->eval(&outer_tuple, &inner_tuple).isFalse())
                {
                    VOLT_TRACE("End Expression evaluated to false, stopping scan");
                    break;
                }
                //
                // Then apply our post-predicate to do further filtering
                //
                if (post_expression == NULL ||
                    post_expression->eval(&outer_tuple, &inner_tuple).isTrue())
                {
                    //
                    // Try to put the tuple into our output table
                    //
                    // This is a bit hacky.  It duplicates the non-eval
                    // world that was here before.  Could fold these two
                    // loops together if we assign table indexes in p_init
                    for (int col_ctr = 0; col_ctr < num_of_outer_cols;
                         ++col_ctr)
                    {
                        join_tuple.setNValue(col_ctr,
                                             m_outputExpressions[col_ctr]->
                                             eval(&outer_tuple, NULL));
                    }
                    //
                    // Append the inner values to the end of our join tuple
                    //
                    for (int col_ctr = num_of_outer_cols;
                         col_ctr < join_tuple.sizeInValues();
                         ++col_ctr)
                    {
                        // For the sake of consistency, we don't try to do
                        // output expressions here with columns from both tables.
                        join_tuple.
                        setNValue(col_ctr,
                                  m_outputExpressions[col_ctr]->
                                  eval(&inner_tuple, NULL));
                    }
                    VOLT_TRACE("join_tuple tuple: %s",
                               join_tuple.debug(output_table->name()).c_str());

                    VOLT_TRACE("MATCH: %s",
                               join_tuple.debug(output_table->name()).c_str());
                    output_table->insertTupleNonVirtual(join_tuple);
                }
            }
        }

        //
        // Left Outer Join
        //
        if (!match && join_type == JOIN_TYPE_LEFT) {
            //
            // Append NULLs to the end of our join tuple
            //
            for (int col_ctr = 0; col_ctr < num_of_inner_cols; ++col_ctr)
            {
                const int index = col_ctr + num_of_outer_cols;
                NValue value = join_tuple.getNValue(index);
                value.setNull();
                join_tuple.setNValue(col_ctr + num_of_outer_cols, value);
            }
            output_table->insertTupleNonVirtual(join_tuple);
        }
    }

    VOLT_TRACE ("result table:\n %s", output_table->debug().c_str());
    VOLT_TRACE("Finished NestLoopIndex");
    return (true);
}

namespace voltdb {
namespace detail {

    struct NestLoopIndexExecutorState
    {
        NestLoopIndexExecutorState(TempTable* outputTable,
            AbstractExecutor* outerExecutor, IndexScanExecutor* innerExecutor,
            const TupleSchema* outerSchema, int outerCols, int innerCols, int searchkeyCols) :
            m_outerExecutor(outerExecutor),
            m_innerExecutor(innerExecutor),
            m_outputTable(outputTable),
            m_outerSchema(outerSchema),
            m_outerTuple(outerSchema),
            m_outerCols(outerCols),
            m_innerCols(innerCols),
            m_searchkeyCols(searchkeyCols),
            m_needNextOuter(true),
            m_hasMatch(false)
        {}

        AbstractExecutor* m_outerExecutor;
        IndexScanExecutor* m_innerExecutor;
        TempTable* m_outputTable;
        const TupleSchema* m_outerSchema;
        TableTuple m_outerTuple;
        int m_outerCols;
        int m_innerCols;
        int m_searchkeyCols;
        bool m_needNextOuter;
        bool m_hasMatch;

    };

} // namespace detail
} // namespace voltdb

NestLoopIndexExecutor::NestLoopIndexExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
    : AbstractExecutor(engine, abstract_node),
    node(NULL), inline_node(NULL), m_lookupType(INDEX_LOOKUP_TYPE_INVALID),
    output_table(NULL), inner_table(NULL), index(NULL), index_values(),
    outer_table(NULL), join_type(), m_outputExpressions(), m_sortDirection(),
    index_values_backing_store(NULL), m_state()
{}

NestLoopIndexExecutor::~NestLoopIndexExecutor()
{
    delete [] index_values_backing_store;
}

bool NestLoopIndexExecutor::support_pull() const
{
    return true;
}

void NestLoopIndexExecutor::p_pre_execute_pull(const NValueArray& params)
{
    assert (node == dynamic_cast<NestLoopIndexPlanNode*>(getPlanNode()));
    assert(node);
    assert (inline_node == dynamic_cast<IndexScanPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_INDEXSCAN)));
    assert(inline_node);

    assert (output_table == dynamic_cast<TempTable*>(node->getOutputTable()));
    assert(output_table);

    //inner_table is the table that has the index to be used in this executor
    assert (inner_table == dynamic_cast<PersistentTable*>(inline_node->getTargetTable()));
    assert(inner_table);
    IndexScanExecutor* inner_executor = dynamic_cast<IndexScanExecutor*>(inline_node->getExecutor());
    assert(inner_executor);
    TableTuple inner_tuple(inner_table->schema());
    int inner_cols = inner_tuple.sizeInValues();
    assert(inner_table->columnCount() == inner_cols);


    // IndexScanPlanNode is inlined and its p_pre_execute_pull is not called.
    inner_executor->set_expressions_pull(params);

    //outer_table is the input table that have tuples to be iterated
    assert(node->getInputTables().size() == 1);
    assert (outer_table == node->getInputTables()[0]);
    assert (outer_table);
    VOLT_TRACE("executing NestLoopIndex with outer table: %s, inner table: %s",
               outer_table->debug().c_str(), inner_table->debug().c_str());
    AbstractExecutor* outer_executor = node->getChildren()[0]->getExecutor();
    TableTuple outer_tuple(outer_table->schema());
    int outer_cols = outer_tuple.sizeInValues();
    assert (outer_table->columnCount() == outer_cols);


    //
    // Substitute parameter to SEARCH KEY Note that the expressions
    // will include TupleValueExpression even after this substitution
    //
    int num_of_searchkeys = (int)inline_node->getSearchKeyExpressions().size();
    for (int ctr = 0; ctr < num_of_searchkeys; ctr++) {
        VOLT_TRACE("Search Key[%d] before substitution:\n%s",
                   ctr, inline_node->getSearchKeyExpressions()[ctr]->debug(true).c_str());

        inline_node->getSearchKeyExpressions()[ctr]->substitute(params);

        VOLT_TRACE("Search Key[%d] after substitution:\n%s",
                   ctr, inline_node->getSearchKeyExpressions()[ctr]->debug(true).c_str());
    }

    m_state.reset(new detail::NestLoopIndexExecutorState(output_table,
        outer_executor, inner_executor, outer_table->schema(),
        outer_cols, inner_cols, num_of_searchkeys));
}

void NestLoopIndexExecutor::p_post_execute_pull() {
    VOLT_TRACE ("result table:\n %s", m_state->m_outputTable->debug().c_str());
    VOLT_TRACE("Finished NestLoopIndex");
}

TableTuple NestLoopIndexExecutor::p_next_pull() {
    //
    // OUTER TABLE ITERATION
    //
    TableTuple joinedTuple = m_state->m_outputTable->tempTuple();
    while (true)
    {
        if (m_state->m_needNextOuter)
        {
            m_state->m_outerTuple = m_state->m_outerExecutor->next_pull();
            if (m_state->m_outerTuple.isNullTuple())
            {
                joinedTuple = TableTuple(m_state->m_outerSchema);
                break;
            }
            VOLT_TRACE("outer_tuple:%s",
                       m_state->m_outerTuple.debug(outer_table->name()).c_str());

            //
            // Try to put the tuple into our output table
            //
            // This is a bit hacky.  It duplicates the non-eval
            // world that was here before.  Could fold these two
            // loops together if we assign table indexes in p_init
            for (int col_ctr = 0; col_ctr < m_state->m_outerCols; ++col_ctr)
            {
                joinedTuple.setNValue(col_ctr,
                    m_outputExpressions[col_ctr]->eval(&m_state->m_outerTuple, NULL));
            }
            // Prepare inner index scan
            reset_inner_state();
            m_state->m_needNextOuter = false;
            m_state->m_hasMatch = false;
        }

        TableTuple innerTuple = m_state->m_innerExecutor->next_pull();
        if (!innerTuple.isNullTuple())
        {
            VOLT_TRACE("inner_tuple:%s",
                       innerTuple.debug(inner_table->name()).c_str());
            //
            // Append the inner values to the end of our join tuple
            //
            for (int col_ctr = m_state->m_outerCols; col_ctr < joinedTuple.sizeInValues(); ++col_ctr)
            {
                // For the sake of consistency, we don't try to do
                // output expressions here with columns from both tables.
                joinedTuple.setNValue(col_ctr,
                    m_outputExpressions[col_ctr]->eval(&innerTuple, NULL));
            }
            VOLT_TRACE("join_tuple tuple: %s",
                       joinedTuple.debug(output_table->name()).c_str());

            VOLT_TRACE("MATCH: %s",
                       joinedTuple.debug(output_table->name()).c_str());

            m_state->m_hasMatch = true;
            break;
        }
        else
        {
            m_state->m_needNextOuter = true;
            //
            // Left Outer Join
            //
            if (join_type == JOIN_TYPE_LEFT && m_state->m_hasMatch == false)
            {
                //
                // Append NULLs to the end of our join tuple
                //
                for (int col_ctr = 0; col_ctr < m_state->m_innerCols; ++col_ctr)
                {
                    const int index = col_ctr + m_state->m_outerCols;
                    NValue value = joinedTuple.getNValue(index);
                    value.setNull();
                    joinedTuple.setNValue(col_ctr + m_state->m_outerCols, value);
                }
                break;
            }
        }
    }

    return joinedTuple;
}

void NestLoopIndexExecutor::reset_inner_state()
{
    //
    // Now use the outer table tuple to construct the search key
    // against the inner table
    //
    //nshi commented this out in revision 4495 of the old repo in index scan executor
    //the code is cut and paste in nest loop and the change is necessary here as well
    //assert (index_values.getSchema()->columnCount() == num_of_searchkeys || m_lookupType == INDEX_LOOKUP_TYPE_GT);
    for (int ctr = m_state->m_searchkeyCols - 1; ctr >= 0 ; --ctr) {
        index_values.setNValue(ctr,
            inline_node->getSearchKeyExpressions()[ctr]->eval(&m_state->m_outerTuple, NULL));
    }
    VOLT_TRACE("Searching %s", index_values.debug("").c_str());

    // Prepare inner index scan
    m_state->m_innerExecutor->set_search_key_pull(index_values, &m_state->m_outerTuple);
}
