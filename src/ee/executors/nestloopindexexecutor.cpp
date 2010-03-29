/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
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
#include "nestloopindexexecutor.h"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "execution/VoltDBEngine.h"
#include "expressions/abstractexpression.h"
#include "plannodes/nestloopindexnode.h"
#include "plannodes/indexscannode.h"
#include "storage/table.h"
#include "storage/persistenttable.h"
#include "storage/temptable.h"
#include "indexes/tableindex.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"

using namespace voltdb;

bool NestLoopIndexExecutor::p_init(AbstractPlanNode* abstract_node,
                                   const catalog::Database* catalog_db, int* tempTableMemoryInBytes)
{
    VOLT_TRACE("init NLIJ Executor");
    assert(tempTableMemoryInBytes);

    node = dynamic_cast<NestLoopIndexPlanNode*>(abstract_node);
    assert(node);
    inline_node = dynamic_cast<IndexScanPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_INDEXSCAN));
    assert(inline_node);
    join_type = node->getJoinType();
    m_lookupType = inline_node->getLookupType();

    //
    // We need exactly one input table and a target table
    //
    assert(node->getInputTables().size() == 1);
    Table* input_table = node->getInputTables()[0];
    assert(input_table);

    Table* target_table = inline_node->getTargetTable();
    assert(target_table);

    //
    // Our output table will have all the columns from outer and inner table
    //
    std::vector<boost::shared_ptr<const TableColumn> > columns;

    // For passing to plan node counterpart
    const TupleSchema *first = input_table->schema();
    const TupleSchema *second = target_table->schema();
    TupleSchema *schema = TupleSchema::createTupleSchema(first, second);

    int combinedColumnCount =
      input_table->columnCount() + target_table->columnCount();
    std::string *columnNames = new std::string[combinedColumnCount];

    std::vector<int> outputColumnGuids;
    int cur_index = 0;
    // copy from outer table (input table)
    for (int col_ctr = 0, col_cnt = input_table->columnCount();
         col_ctr < col_cnt;
         col_ctr++, cur_index++)
    {
        columnNames[cur_index] = input_table->columnName(col_ctr);
        outputColumnGuids.
            push_back(node->getChildren()[0]->getOutputColumnGuids()[col_ctr]);
    }

    // copy from inner table (target table)
    for (int col_ctr = 0, col_cnt = target_table->columnCount();
         col_ctr < col_cnt;
         col_ctr++, cur_index++)
    {
        columnNames[cur_index] = target_table->columnName(col_ctr);
        outputColumnGuids.push_back(inline_node->getOutputColumnGuids()[col_ctr]);
    }

    // create the output table
    node->setOutputTable(TableFactory::getTempTable(node->getInputTables()[0]->databaseId(),
                                                    "temp", schema, columnNames, tempTableMemoryInBytes));

    // Set the mapping of column names to column indexes in output tables
    node->setOutputColumnGuids(outputColumnGuids);
    // clean up
    delete[] columnNames;

    //
    // Make sure that we actually search keys
    //
    int num_of_searchkeys = (int)inline_node->getSearchKeyExpressions().size();
    if (num_of_searchkeys == 0) {
        VOLT_ERROR("There are no search key expressions for the internal"
                   " PlanNode '%s' of PlanNode '%s'",
                   inline_node->debug().c_str(), node->debug().c_str());
        return false;
    }
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

    return true;
}

bool NestLoopIndexExecutor::p_execute(const NValueArray &params)
{
    VOLT_TRACE ("executing NestLoopIndex...");
    assert (node == dynamic_cast<NestLoopIndexPlanNode*>(abstract_node));
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
    VOLT_TRACE ("outer table:\n %s", outer_table->debug().c_str());
    VOLT_TRACE ("inner table:\n %s", inner_table->debug().c_str());

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
    TableIterator outer_iterator(outer_table);
    int num_of_outer_cols = outer_table->columnCount();
    int num_of_inner_cols = inner_table->columnCount();
    assert (outer_tuple.sizeInValues() == outer_table->columnCount());
    assert (inner_tuple.sizeInValues() == inner_table->columnCount());
    TableTuple &join_tuple = output_table->tempTuple();
    while (outer_iterator.next(outer_tuple)) {
        VOLT_TRACE("outer_tuple:%s",
                   outer_tuple.debug(outer_table->name()).c_str());

        //
        // Now use the outer table tuple to construct the search key
        // against the inner table
        //
        assert (index_values.getSchema()->columnCount() == num_of_searchkeys || m_lookupType == INDEX_LOOKUP_TYPE_GT);
        for (int ctr = num_of_searchkeys - 1; ctr >= 0 ; --ctr) {
            index_values.
              setNValue(ctr,
                        inline_node->getSearchKeyExpressions()[ctr]->eval(&outer_tuple, NULL));
        }
        VOLT_TRACE("Searching %s", index_values.debug("").c_str());

        //
        // In order to apply the Expression trees in our join, we need
        // to put the outer and inner tuples together into a single
        // tuple.  The column references in the Expressions have
        // already been offset to accomodate this
        //
        for (int col_ctr = 0; col_ctr < num_of_outer_cols; ++col_ctr) {
            join_tuple.setNValue(col_ctr, outer_tuple.getNValue(col_ctr));
        }

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
        if (m_lookupType == INDEX_LOOKUP_TYPE_EQ) {
            index->moveToKey(&index_values);
        } else if (m_lookupType == INDEX_LOOKUP_TYPE_GT) {
            index->moveToGreaterThanKey(&index_values);
        } else if (m_lookupType == INDEX_LOOKUP_TYPE_GTE) {
            index->moveToKeyOrGreater(&index_values);
        } else {
            return false;
        }

        bool match = false;
        while ((m_lookupType == INDEX_LOOKUP_TYPE_EQ &&
                !(inner_tuple = index->nextValueAtKey()).isNullTuple()) ||
               (m_lookupType != INDEX_LOOKUP_TYPE_EQ &&
                !(inner_tuple = index->nextValue()).isNullTuple()))
        {
            match = true;

            VOLT_TRACE("inner_tuple:%s",
                       inner_tuple.debug(inner_table->name()).c_str());
            //
            // Append the inner values to the end of our join tuple
            //
            for (int col_ctr = 0; col_ctr < num_of_inner_cols; ++col_ctr)
            {
                join_tuple.setNValue(col_ctr + num_of_outer_cols,
                                     inner_tuple.getNValue(col_ctr));
            }
            VOLT_TRACE("join_tuple tuple: %s",
                       join_tuple.debug(output_table->name()).c_str());

            //
            // First check whether the end_expression is now false
            //
            if (end_expression != NULL &&
                end_expression->eval(&join_tuple, NULL).isFalse())
            {
                VOLT_TRACE("End Expression evaluated to false, stopping scan");
                break;
            }
            //
            // Then apply our post-predicate to do further filtering
            //
            if (post_expression == NULL ||
                post_expression->eval(&join_tuple, NULL).isTrue())
            {
                //
                // Try to put the tuple into our output table
                //
                VOLT_TRACE("MATCH: %s",
                           join_tuple.debug(output_table->name()).c_str());
                output_table->insertTupleNonVirtual(join_tuple);
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
    return (true);
}

NestLoopIndexExecutor::~NestLoopIndexExecutor() {
    delete [] index_values_backing_store;
}
