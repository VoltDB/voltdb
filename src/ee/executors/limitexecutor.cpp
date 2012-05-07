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

#include "limitexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "plannodes/limitnode.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"

namespace voltdb {
namespace detail {

    struct LimitExecutorState
    {   
        LimitExecutorState(LimitPlanNode* node, Table* targetTable, const NValueArray &params):
            m_limit(0), m_offset(0), m_tupleCtr(0), m_tupleSkipped(0), 
            m_targetTableSchema(targetTable->schema()),
            m_childExecutor(NULL),
            m_iterator(targetTable->iterator())
        {
            node->getLimitAndOffsetByReference(params, m_limit, m_offset);
            assert(m_limit >= 0);
            assert(m_offset >= 0);
            std::vector<AbstractPlanNode*>& children = node->getChildren();
            assert(children.size() == 1);
            m_childExecutor = children[0]->getExecutor();
            assert(m_childExecutor);
        }
            
        int m_limit;
        int m_offset;
        int m_tupleCtr;
        int m_tupleSkipped;
        const TupleSchema* m_targetTableSchema; 
        AbstractExecutor* m_childExecutor;
        TableIterator m_iterator;
    };
    
} // namespace detail
} // namespace voltdb

using namespace voltdb;

LimitExecutor::LimitExecutor(VoltDBEngine* engine, AbstractPlanNode* abstract_node)
    : AbstractExecutor(engine, abstract_node), m_state()
{}


bool
LimitExecutor::p_init(AbstractPlanNode* abstract_node,
                      TempTableLimits* limits)
{
    VOLT_TRACE("init limit Executor");

    LimitPlanNode* node = dynamic_cast<LimitPlanNode*>(abstract_node);
    assert(node);

    //
    // Skip if we are inline
    //
    if (!node->isInline())
    {
        //
        // Just copy the table schema of our input table
        //
        assert(node->getInputTables().size() == 1);
        node->
            setOutputTable(TableFactory::
                           getCopiedTempTable(node->databaseId(),
                                              node->getInputTables()[0]->name(),
                                              node->getInputTables()[0],
                                              limits));
    }
    return true;
}

bool
LimitExecutor::p_execute(const NValueArray &params)
{
    LimitPlanNode* node = dynamic_cast<LimitPlanNode*>(m_abstractNode);
    assert(node);
    Table* output_table = node->getOutputTable();
    assert(output_table);
    Table* input_table = node->getInputTables()[0];
    assert(input_table);

    //
    // Grab the iterator for our input table, and loop through until
    // we have copy enough tuples for the limit specified by the node
    //
    TableTuple tuple(input_table->schema());
    TableIterator iterator = input_table->iterator();
    int tuple_ctr = 0;

    int limit = 0, offset = 0;
    node->getLimitAndOffsetByReference(params, limit, offset);

    bool start = (offset == 0);
    while (iterator.next(tuple) && (tuple_ctr < limit))
    {
        // TODO: need a way to skip / iterate N items.
        if (start) {
            if (!output_table->insertTuple(tuple))
            {
                VOLT_ERROR("Failed to insert tuple from input table '%s' into"
                           " output table '%s'",
                           input_table->name().c_str(),
                           output_table->name().c_str());
                return false;
            }
            tuple_ctr++;
        } else
        {
            start = (iterator.getLocation() >= offset);
        }
    }

    return true;
}


//@TODO pullexec prototype
TableTuple LimitExecutor::p_next_pull() {

    TableTuple tuple(m_state->m_targetTableSchema);
    
    // Skip first offset tuples
    do {
        tuple = m_state->m_childExecutor->p_next_pull();
    } while (m_state->m_tupleSkipped++ < m_state->m_offset && tuple.isNullTuple() == false);
     
    // Check if we hit the limit
    if (tuple.isNullTuple() == true || m_state->m_tupleCtr >= m_state->m_limit) {
        // We either exhausted the input table or reached the limit 
        tuple = TableTuple(m_state->m_targetTableSchema);
    } else {
        ++m_state->m_tupleCtr;
    }
    return tuple;
}

void LimitExecutor::p_pre_execute_pull(const NValueArray &params) {
    //
    // Initialize itself
    //
    LimitPlanNode* node = dynamic_cast<LimitPlanNode*>(m_abstractNode);
    assert(node);
    Table* input_table = node->getInputTables()[0];
    assert(input_table);
    
    m_state.reset(new detail::LimitExecutorState(node, input_table, params));    
}

bool LimitExecutor::support_pull() const {
    return true;
}
