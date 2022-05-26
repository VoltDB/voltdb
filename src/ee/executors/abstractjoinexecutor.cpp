/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
#include "abstractjoinexecutor.h"
#include "executors/aggregateexecutor.h"
#include "plannodes/abstractjoinnode.h"

using namespace std;
using namespace voltdb;

void AbstractJoinExecutor::outputTuple(CountingPostfilter& postfilter, TableTuple& join_tuple, ProgressMonitorProxy& pmp) {
    if (m_aggExec != NULL) {
        m_aggExec->p_execute_tuple(join_tuple);
        return;
    }
    m_tmpOutputTable->insertTempTuple(join_tuple);
    pmp.countdownProgress();
}

void AbstractJoinExecutor::p_init_null_tuples(Table* outer_table, Table* inner_table) {
    if (m_joinType != JOIN_TYPE_INNER) {
        vassert(inner_table);
        m_null_inner_tuple.init(inner_table->schema());
        if (m_joinType == JOIN_TYPE_FULL) {
            vassert(outer_table);
            m_null_outer_tuple.init(outer_table->schema());
        }
    }
}

bool AbstractJoinExecutor::p_init(AbstractPlanNode* abstract_node,
                                  const ExecutorVector& executorVector)
{
    VOLT_TRACE("Init AbstractJoinExecutor Executor");

    AbstractJoinPlanNode* node = dynamic_cast<AbstractJoinPlanNode*>(abstract_node);
    vassert(node);

    m_joinType = node->getJoinType();
    vassert(m_joinType == JOIN_TYPE_INNER || m_joinType == JOIN_TYPE_LEFT || m_joinType == JOIN_TYPE_FULL);

    // Create output table based on output schema from the plan
    setTempOutputTable(executorVector);
    vassert(m_tmpOutputTable);

    // Inline aggregation can be serial, partial or hash
    m_aggExec = voltdb::getInlineAggregateExecutor(m_abstractNode);

    return true;
}
