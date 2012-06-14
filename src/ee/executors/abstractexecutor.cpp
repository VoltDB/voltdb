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

#include "abstractexecutor.h"

#include "execution/VoltDBEngine.h"
#include "plannodes/abstractoperationnode.h"
#include "plannodes/abstractscannode.h"
#include <vector>

using namespace std;
using namespace voltdb;

bool AbstractExecutor::init(VoltDBEngine* engine,
                            TempTableLimits* limits)
{
    assert (m_abstractNode);
    //
    // Grab the input tables directly from this node's children
    //
    vector<Table*> input_tables;
    for (int ctr = 0,
             cnt = static_cast<int>(m_abstractNode->getChildren().size());
         ctr < cnt;
         ctr++)
    {
        Table* table = m_abstractNode->getChildren()[ctr]->getOutputTable();
        if (table == NULL) {
            VOLT_ERROR("Output table from PlanNode '%s' is NULL",
                       m_abstractNode->getChildren()[ctr]->debug().c_str());
            return false;
        }
        input_tables.push_back(table);
    }
    m_abstractNode->setInputTables(input_tables);

    // Some tables have target tables (scans + operations) that are
    // based on tables under the control of the local storage manager
    // (as opposed to an intermediate result table). We'll grab them
    // from the VoltDBEngine. This is kind of a hack job here... is
    // there a better way?

    AbstractScanPlanNode* scan_node =
        dynamic_cast<AbstractScanPlanNode*>(m_abstractNode);
    AbstractOperationPlanNode* oper_node =
        dynamic_cast<AbstractOperationPlanNode*>(m_abstractNode);
    if (scan_node || oper_node)
    {
        Table* target_table = NULL;

        string targetTableName;
        if (scan_node) {
            targetTableName = scan_node->getTargetTableName();
            target_table = scan_node->getTargetTable();
        } else if (oper_node) {
            targetTableName = oper_node->getTargetTableName();
            target_table = oper_node->getTargetTable();
        }

        // If the target_table is NULL, then we need to ask the engine
        // for a reference to what we need
        // Really, we can't enforce this when we load the plan? --izzy 7/3/2010
        if (target_table == NULL) {
            target_table = engine->getTable(targetTableName);
            if (target_table == NULL) {
                VOLT_ERROR("Failed to retrieve target table '%s' "
                           "from execution engine for PlanNode '%s'",
                           targetTableName.c_str(),
                           m_abstractNode->debug().c_str());
                return false;
            }
            if (scan_node) {
                scan_node->setTargetTable(target_table);
            } else if (oper_node) {
                oper_node->setTargetTable(target_table);
            }
        }
    }
    needs_outputtable_clear_cached = needsOutputTableClear();

    // Call the p_init() method on our derived class
    try {
        if (!p_init(m_abstractNode, limits))
            return false;
    } catch (exception& err) {
        char message[128];
        snprintf(message, 128, "The Executor failed to initialize PlanNode '%s'",
                m_abstractNode->debug().c_str());
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      message);
    }
    Table* tmp_output_table_base = m_abstractNode->getOutputTable();
    m_tmpOutputTable = dynamic_cast<TempTable*>(tmp_output_table_base);

    // determines whether the output table should be cleared or not.
    // specific executor might not need (and must not do) clearing.
    if (!needs_outputtable_clear_cached) {
        VOLT_TRACE("Did not clear output table because the derived class"
                   " answered so");
        m_tmpOutputTable = NULL;
    }
    return true;
}

namespace voltdb {
namespace detail {
struct AbstractExecutorState
{
    AbstractExecutorState(Table* table) :
        m_iterator(table->makeIterator()),
        m_nextTuple(table->schema()),
        m_nullTuple(table->schema())
    {}
    boost::scoped_ptr<TableIterator> m_iterator;
    TableTuple m_nextTuple;
    TableTuple m_nullTuple;
};

} // namespace detail
} // namespace voltdb

using namespace voltdb::detail;

AbstractExecutor::AbstractExecutor(VoltDBEngine* engine, AbstractPlanNode* abstractNode):
    m_abstractNode(abstractNode), m_tmpOutputTable(NULL), m_absState()
{}

AbstractExecutor::~AbstractExecutor() {}

// Top-level entry point for executor pull protocol
bool AbstractExecutor::execute_pull(const NValueArray& params)
{
    assert(getPlanNode());
    VOLT_TRACE("Starting execution of plannode(id=%d)...",
               getPlanNode()->getPlanNodeId());

    // hook to give executor a chance to perform some initialization if necessary
    // potentially could be used to call children in push mode
    // recurs to children
    boost::function<void(AbstractExecutor*)> fpreexecute =
        boost::bind(&AbstractExecutor::pre_execute_pull, _1, boost::cref(params));
    depth_first_iterate_pull(fpreexecute, true);

    // run the executor
    p_execute_pull();

    // some executors need to do some work after the iteration
    boost::function<void(AbstractExecutor*)> fpostexecute =
        &AbstractExecutor::post_execute_pull;
    depth_first_iterate_pull(fpostexecute, true);

    return true;
}

static void add_to_list(AbstractExecutor* exec, std::vector<AbstractExecutor*>& list)
{
    list.push_back(exec);
}

//@TODO To accomodate executors that have not been updated to implement the pull protocol,
// this implementation provides an adaptor to the older push protocol.
// This allows VoltDBEngine to switch over immediately to using the pull protocol.
// Any node that does not actually implement a custom p_pre_execute_pull/p_next_pull
// instead inherits this sub-optimal default behavior AbstractExecutor that:
// Implements p_pre_execute_pull to do the following:
// - Recursively constructs a (depth-first) list of its child(ren).
// - Calls execute on each of them, and finally itself.
// Implements p_next_pull to retrieve each row from its output table (previously populated by its execute method).
void AbstractExecutor::p_pre_execute_pull(const NValueArray& params)
{
    // Build the depth-first children list.
    std::vector<AbstractExecutor*> execs;
    boost::function<void(AbstractExecutor*)> faddtolist =
        boost::bind(&add_to_list, _1, boost::ref(execs));
    // The second parameter (stop when hit non-pull aware executor) is false here
    // because we want to call the push executor on the full list of children.
    depth_first_iterate_pull(faddtolist, false);

    // Walk through the queue and execute each plannode.  The query
    // planner guarantees that for a given plannode, all of its
    // children are positioned before it in this list, therefore
    // dependency tracking is not needed here.
    size_t ttl = execs.size();
    for (size_t ctr = 0; ctr < ttl; ++ctr)
    {
        AbstractExecutor* executor = execs[ctr];
        assert(executor);

        if (!executor->execute(params))
        {
            VOLT_TRACE("The Executor's execution failed");
            // set these back to -1 for error handling
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          "The Executor's execution failed");
        }
    }

    // Now the executor's output table is populated.
    // Initialize the iterator to be ready for p_next_pull.
    Table* output_table = m_abstractNode->getOutputTable();
    assert(output_table);
    m_absState.reset(new detail::AbstractExecutorState(output_table));
}

TableTuple AbstractExecutor::p_next_pull()
{
    if (m_absState->m_iterator->next(m_absState->m_nextTuple)) {
        return m_absState->m_nextTuple;
    }
    return m_absState->m_nullTuple;
}
