/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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

#pragma once

#include "common/common.h"
#include "common/tabletuple.h"
#include "common/valuevector.h"
#include "executors/abstractexecutor.h"
#include <vector>

namespace voltdb {

    class AbstractTempTable;
    class OrderByPlanNode;
    class LimitPlanNode;
    class AggregateExecutorBase;
    class ProgressMonitorProxy;
    class CountingPostfilter;

    /**
     * The optimized replacement for an ORDER BY executor to be used at the coordinator node
     * to merge-sort results from multiple partitions. The assumption is
     * that the individual partitions results are already sorted in the order
     * specified by the inlined OrderByPlanNode
     */
    class MergeReceiveExecutor : public AbstractExecutor {
        OrderByPlanNode* m_orderby_node;
        LimitPlanNode* m_limit_node;
        AggregateExecutorBase* m_agg_exec;
    public:
        MergeReceiveExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node);

        // Public for testing purpose only
        static void merge_sort(const std::vector<TableTuple>& tuples,
                               std::vector<int64_t>& partitionTupleCounts,
                               AbstractExecutor::TupleComparer comp,
                               CountingPostfilter& postfilter,
                               AggregateExecutorBase* agg_exec,
                               AbstractTempTable* output_table,
                               ProgressMonitorProxy* pmp);
    protected:
        bool p_init(AbstractPlanNode* abstract_node,
                    const ExecutorVector& executorVector);
        bool p_execute(const NValueArray &params);
    };

}

