/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

#include "executorutil.h"

#include "common/debuglog.h"
#include "common/FatalException.hpp"
#include "executors/executors.h"
#include <cassert>

namespace voltdb {

AbstractExecutor* getNewExecutor(VoltDBEngine *engine,
                                 AbstractPlanNode* abstract_node) {
    PlanNodeType type = abstract_node->getPlanNodeType();
    switch (type) {
    case PLAN_NODE_TYPE_AGGREGATE: return new AggregateExecutor<PLAN_NODE_TYPE_AGGREGATE>(engine, abstract_node);
    case PLAN_NODE_TYPE_HASHAGGREGATE: return new AggregateExecutor<PLAN_NODE_TYPE_HASHAGGREGATE>(engine, abstract_node);
    case PLAN_NODE_TYPE_DELETE: return new DeleteExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_DISTINCT: return new DistinctExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_INDEXSCAN: return new IndexScanExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_INSERT: return new InsertExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_LIMIT: return new LimitExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_MATERIALIZE: return new MaterializeExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_NESTLOOP: return new NestLoopExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_NESTLOOPINDEX: return new NestLoopIndexExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_ORDERBY: return new OrderByExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_PROJECTION: return new ProjectionExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_RECEIVE: return new ReceiveExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_SEND: return new SendExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_SEQSCAN: return new SeqScanExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_UNION: return new UnionExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_UPDATE: return new UpdateExecutor(engine, abstract_node);
    default:
        VOLT_ERROR( "Invalid PlannodeType %d", (int) type);
    }
    return NULL;
}

}
