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

#include "receiveexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "plannodes/receivenode.h"
#include "execution/VoltDBEngine.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"

namespace voltdb {

namespace detail {

    struct ReceiveExecutorState
    {
        ReceiveExecutorState(Table* outputTable) :
            m_outputTable(outputTable), m_iterator(outputTable->iterator()),
            m_targetTableSchema(outputTable->schema())
        {}

        Table* m_outputTable;
        TableIterator m_iterator;
        const TupleSchema* m_targetTableSchema;
    };
} // namespace detail

bool ReceiveExecutor::p_init(AbstractPlanNode* abstract_node,
                             TempTableLimits* limits)
{
    VOLT_TRACE("init Receive Executor");
    assert(limits);

    ReceivePlanNode* node = dynamic_cast<ReceivePlanNode*>(abstract_node);
    assert(node);

    //
    // Construct the output table
    //
    TupleSchema* schema = node->generateTupleSchema(true);
    int num_of_columns = static_cast<int>(node->getOutputSchema().size());
    std::string* column_names = new std::string[num_of_columns];
    for (int ctr = 0; ctr < num_of_columns; ctr++)
    {
        column_names[ctr] = node->getOutputSchema()[ctr]->getColumnName();
    }
    node->setOutputTable(TableFactory::getTempTable(node->databaseId(),
                                                    "temp",
                                                    schema,
                                                    column_names,
                                                    limits));
    delete[] column_names;
    return true;
}

bool ReceiveExecutor::p_execute(const NValueArray &params) {
    int loadedDeps = 0;
    ReceivePlanNode* node = dynamic_cast<ReceivePlanNode*>(getPlanNode());
    Table* output_table = dynamic_cast<Table*>(node->getOutputTable());

    // iterate dependencies stored in the frontend and union them
    // into the output_table. The engine does this work for peanuts.

    // todo: should pass the transaction's string pool through
    // as the underlying table loader would use it.
    do {
        loadedDeps =
        m_engine->loadNextDependency(output_table);
    } while (loadedDeps > 0);

    return true;
}

ReceiveExecutor::ReceiveExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
    : AbstractExecutor(engine, abstract_node), m_engine(engine), m_state()
{}

ReceiveExecutor::~ReceiveExecutor() {
}

//@TODO pullexec prototype
TableTuple ReceiveExecutor::p_next_pull() {
    TableTuple tuple(m_state->m_targetTableSchema);
    if (!m_state->m_iterator.next(tuple))
        tuple = TableTuple(m_state->m_targetTableSchema);
    return tuple;
}

void ReceiveExecutor::p_pre_execute_pull(const NValueArray &params) {
    int loadedDeps = 0;
    ReceivePlanNode* node = dynamic_cast<ReceivePlanNode*>(getPlanNode());
    Table* output_table = dynamic_cast<Table*>(node->getOutputTable());

    // iterate dependencies stored in the frontend and union them
    // into the output_table. The engine does this work for peanuts.

    // todo: should pass the transaction's string pool through
    // as the underlying table loader would use it.
    do {
        loadedDeps = m_engine->loadNextDependency(output_table);
    } while (loadedDeps > 0);

    m_state.reset(new detail::ReceiveExecutorState(output_table));
}

bool ReceiveExecutor::support_pull() const {
    return true;
}

void ReceiveExecutor::p_reset_state_pull() {
    m_state->m_iterator = m_state->m_outputTable->iterator();
}

}
