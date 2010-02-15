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

#include "projectionexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"
#include "plannodes/projectionnode.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"
#include "storage/temptable.h"

namespace voltdb {

bool ProjectionExecutor::p_init(AbstractPlanNode *abstract_node, const catalog::Database* catalog_db, int* tempTableMemoryInBytes) {
    VOLT_TRACE("init Projection Executor");
    assert(tempTableMemoryInBytes);

    ProjectionPlanNode* node = dynamic_cast<ProjectionPlanNode*>(abstract_node);
    assert(node);

    //
    // Construct the output table
    //
    num_of_columns = (int) node->getOutputColumnNames().size();
    assert(num_of_columns > 0);
    assert(num_of_columns == node->getOutputColumnTypes().size());
    assert(num_of_columns == node->getOutputColumnSizes().size());
    const std::vector<voltdb::ValueType> outputColumnTypes = node->getOutputColumnTypes();
    const std::vector<std::string> outputColumnNames = node->getOutputColumnNames();
    const std::vector<uint16_t> outputColumnSizes = node->getOutputColumnSizes();
    const std::vector<bool> outputColumnAllowNull(num_of_columns, true);
    TupleSchema *schema = TupleSchema::createTupleSchema(outputColumnTypes, outputColumnSizes, outputColumnAllowNull, true);
    std::string *columnNames = new std::string[num_of_columns];
    for (int ctr = 0; ctr < num_of_columns; ctr++) {
        columnNames[ctr] = node->getOutputColumnNames()[ctr];
    }
    node->setOutputTable(TableFactory::getTempTable(node->databaseId(), "temp", schema, columnNames, tempTableMemoryInBytes));

    delete[] columnNames;

    // initialize local variables
    all_tuple_array_ptr =
      expressionutil::convertIfAllTupleValues(node->getOutputColumnExpressions());
    all_tuple_array = all_tuple_array_ptr.get();
    all_param_array_ptr =
      expressionutil::convertIfAllParameterValues(node->getOutputColumnExpressions());
    all_param_array = all_param_array_ptr.get();

    needs_substitute_ptr = boost::shared_array<bool>(new bool[num_of_columns]);
    needs_substitute = needs_substitute_ptr.get();
    typedef AbstractExpression* ExpRawPtr;
    expression_array_ptr = boost::shared_array<ExpRawPtr>(new ExpRawPtr[num_of_columns]);
    expression_array = expression_array_ptr.get();
    for (int ctr = 0; ctr < num_of_columns; ctr++) {
        assert (node->getOutputColumnExpressions()[ctr] != NULL);
        expression_array_ptr[ctr] = node->getOutputColumnExpressions()[ctr];
        needs_substitute_ptr[ctr] = node->getOutputColumnExpressions()[ctr]->hasParameter();
    }


    output_table = dynamic_cast<TempTable*>(node->getOutputTable()); //output table should be temptable

    if (!node->isInline()) {
        input_table = node->getInputTables()[0];
        tuple = TableTuple(input_table->schema());
    }
    return (true);
}

bool ProjectionExecutor::p_execute(const NValueArray &params) {
#ifndef NDEBUG
    ProjectionPlanNode* node = dynamic_cast<ProjectionPlanNode*>(abstract_node);
#endif
    assert (node);
    assert (!node->isInline()); // inline projection's execute() should not be called
    assert (output_table == dynamic_cast<TempTable*>(node->getOutputTable()));
    assert (output_table);
    assert (input_table == node->getInputTables()[0]);
    assert (input_table);

    VOLT_TRACE("INPUT TABLE: %s\n", input_table->debug().c_str());

    //
    // Since we have the input params, we need to call substitute to change any nodes in our expression tree
    // to be ready for the projection operations in execute
    //
    assert (num_of_columns == (int)node->getOutputColumnNames().size());
    if (all_tuple_array == NULL && all_param_array == NULL) {
        for (int ctr = num_of_columns - 1; ctr >= 0; --ctr) {
            assert(expression_array[ctr]);
            expression_array[ctr]->substitute(params);
            VOLT_TRACE("predicate[%d]: %s", ctr, expression_array[ctr]->debug(true).c_str());
        }
    }

    //
    // Now loop through all the tuples and push them through our output expression
    // This will generate new tuple values that we will insert into our output table
    //
    TableIterator iterator(input_table);
    assert (tuple.sizeInValues() == input_table->columnCount());
    while (iterator.next(tuple)) {
        //
        // Project (or replace) values from input tuple
        //
        TableTuple &temp_tuple = output_table->tempTuple();
        if (all_tuple_array != NULL) {
            VOLT_TRACE("sweet, all tuples");
            for (int ctr = num_of_columns - 1; ctr >= 0; --ctr) {
                temp_tuple.setNValue(ctr, tuple.getNValue(all_tuple_array[ctr]));
            }
        } else if (all_param_array != NULL) {
            VOLT_TRACE("sweet, all params");
            for (int ctr = num_of_columns - 1; ctr >= 0; --ctr) {
                temp_tuple.setNValue(ctr, params[all_param_array[ctr]]);
            }
        } else {
            for (int ctr = num_of_columns - 1; ctr >= 0; --ctr) {
                temp_tuple.setNValue(ctr, expression_array[ctr]->eval(&tuple, NULL));
            }
        }
        output_table->insertTupleNonVirtual(temp_tuple);
        /*if (!output_table->insertTupleNonVirtual(temp_tuple)) {
            // TODO: DEBUG
            VOLT_ERROR("Failed to insert projection tuple from input table '%s' into output table '%s'", input_table->name().c_str(), output_table->name().c_str());
            return (false);
        }*/
    }

    //VOLT_TRACE("PROJECTED TABLE: %s\n", output_table->debug().c_str());

    return (true);
}

ProjectionExecutor::~ProjectionExecutor() {
}

}
