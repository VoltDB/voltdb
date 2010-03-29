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
#include <stack>
#include "nestloopexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "expressions/abstractexpression.h"
#include "expressions/tuplevalueexpression.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"
#include "plannodes/nestloopnode.h"

#ifdef VOLT_DEBUG_ENABLED
#include <ctime>
#include <sys/times.h>
#include <unistd.h>
#endif

namespace voltdb {

bool
assignTupleValueIndex(AbstractExpression *ae,
                      const std::string &oname,
                      const std::string &iname)
{
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
        VOLT_ERROR("TableTupleValue in join with unknown table name.");
        return false;
    }

    return true;
}

bool NestLoopExecutor::p_init(AbstractPlanNode* abstract_node, const catalog::Database* catalog_db, int* tempTableMemoryInBytes) {
    VOLT_TRACE("init NestLoop Executor");
    assert(tempTableMemoryInBytes);

    NestLoopPlanNode* node = dynamic_cast<NestLoopPlanNode*>(abstract_node);
    assert(node);

    // produce the fully joined schema relying on a later projection
    // to narrow the output later as required.
    assert(node->getInputTables().size() == 2);
    const TupleSchema *first = node->getInputTables()[0]->schema();
    const TupleSchema *second = node->getInputTables()[1]->schema();
    TupleSchema *schema = TupleSchema::createTupleSchema(first, second);

    int combinedColumnCount = first->columnCount() + second->columnCount();
    std::string *columnNames = new std::string[combinedColumnCount];
    std::vector<int> outputColumnGuids;
    int index = 0;

    for (int ctr = 0; ctr < 2; ctr++) {
        assert(node->getInputTables()[ctr]);
        for (int col_ctr = 0, col_cnt = node->getInputTables()[ctr]->columnCount();
             col_ctr < col_cnt;
             col_ctr++, index++)
        {
            outputColumnGuids.
                push_back(node->getChildren()[ctr]->getOutputColumnGuids()[col_ctr]);
            columnNames[index] = node->getInputTables()[ctr]->columnName(col_ctr);
        }
    }

    // Set the mapping of column names to column indexes in output tables
    node->setOutputColumnGuids(outputColumnGuids);

    // create the output table
    node->setOutputTable(
        TableFactory::getTempTable(
            node->getInputTables()[0]->databaseId(), "temp", schema, columnNames, tempTableMemoryInBytes));

    // for each tuple value expression in the predicate, determine
    // which tuple is being represented. Tuple could come from outer
    // table or inner table. Configure the predicate to use the correct
    // eval() tuple parameter. By convention, eval's first parameter
    // will always be the outer table and its second parameter the inner
    const AbstractExpression *predicate = node->getPredicate();
    std::stack<const AbstractExpression*> stack;
    while (predicate != NULL) {
        const AbstractExpression *left = predicate->getLeft();
        const AbstractExpression *right = predicate->getRight();

        if (right != NULL) {
            if (right->getExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE) {
                if (!assignTupleValueIndex(const_cast<AbstractExpression*>(right),
                                           node->getInputTables()[0]->name(),
                                           node->getInputTables()[1]->name())) {
                    delete [] columnNames;
                    return false;
                }
            }
            // remember the right node - must visit its children
            stack.push(right);
        }
        if (left != NULL) {
            if (left->getExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE) {
                if (!assignTupleValueIndex(const_cast<AbstractExpression*>(left),
                                           node->getInputTables()[0]->name(),
                                           node->getInputTables()[1]->name())) {
                    delete [] columnNames;
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

    delete[] columnNames;
    return true;
}


bool NestLoopExecutor::p_execute(const NValueArray &params) {
    VOLT_DEBUG("executing NestLoop...");

    NestLoopPlanNode* node = dynamic_cast<NestLoopPlanNode*>(abstract_node);
    assert(node);
    assert(node->getInputTables().size() == 2);

    Table* output_table_ptr = node->getOutputTable();
    assert(output_table_ptr);

    // output table must be a temp table
    TempTable* output_table = dynamic_cast<TempTable*>(output_table_ptr);
    assert(output_table);

    Table* outer_table = node->getInputTables()[0];
    assert(outer_table);

    Table* inner_table = node->getInputTables()[1];
    assert(inner_table);

    VOLT_TRACE ("input table left:\n %s", outer_table->debug().c_str());
    VOLT_TRACE ("input table right:\n %s", inner_table->debug().c_str());

    //
    // Join Expression
    //
    AbstractExpression *predicate = node->getPredicate();
    if (predicate) {
        predicate->substitute(params);
        VOLT_TRACE ("predicate: %s", predicate == NULL ?
                    "NULL" : predicate->debug(true).c_str());
    }

    int outer_cols = outer_table->columnCount();
    int inner_cols = inner_table->columnCount();
    TableTuple outer_tuple(node->getInputTables()[0]->schema());
    TableTuple inner_tuple(node->getInputTables()[1]->schema());
    TableTuple &joined = output_table->tempTuple();

    TableIterator iterator0(outer_table);
    while (iterator0.next(outer_tuple)) {

        // populate output table's temp tuple with outer table's values
        // probably have to do this at least once - avoid doing it many
        // times per outer tuple
        for (int col_ctr = 0; col_ctr < outer_cols; col_ctr++) {
            joined.setNValue(col_ctr, outer_tuple.getNValue(col_ctr));
        }

        TableIterator iterator1(inner_table);
        while (iterator1.next(inner_tuple)) {
            if (predicate == NULL || predicate->eval(&outer_tuple, &inner_tuple).isTrue()) {
                // Matched! Complete the joined tuple with the inner column values.
                for (int col_ctr = 0; col_ctr < inner_cols; col_ctr++) {
                    joined.setNValue(col_ctr + outer_cols, inner_tuple.getNValue(col_ctr));
                }
                output_table->insertTupleNonVirtual(joined);
            }
        }
    }

    return (true);
}

}
