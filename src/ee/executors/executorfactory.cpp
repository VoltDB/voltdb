/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include "executorfactory.h"

#include "common/debuglog.h"
#include "common/FatalException.hpp"

#include "executors/abstractexecutor.h"
#include "executors/aggregateexecutor.h"
#include "executors/deleteexecutor.h"
#include "executors/indexscanexecutor.h"
#include "executors/indexcountexecutor.h"
#include "executors/tablecountexecutor.h"
#include "executors/insertexecutor.h"
#include "executors/limitexecutor.h"
#include "executors/materializeexecutor.h"
#include "executors/materializedscanexecutor.h"
#include "executors/mergereceiveexecutor.h"
#include "executors/nestloopexecutor.h"
#include "executors/nestloopindexexecutor.h"
#include "executors/orderbyexecutor.h"
#include "executors/projectionexecutor.h"
#include "executors/receiveexecutor.h"
#include "executors/sendexecutor.h"
#include "executors/seqscanexecutor.h"
#include "executors/tuplescanexecutor.h"
#include "executors/unionexecutor.h"
#include "executors/updateexecutor.h"
#include "executors/windowfunctionexecutor.h"

namespace voltdb {

AbstractExecutor* getNewExecutor(VoltDBEngine *engine,
                                 AbstractPlanNode* abstract_node) {
    PlanNodeType type = abstract_node->getPlanNodeType();
    switch (type) {
    case PLAN_NODE_TYPE_AGGREGATE: return new AggregateSerialExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_DELETE: return new DeleteExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_HASHAGGREGATE: return new AggregateHashExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_PARTIALAGGREGATE: return new AggregatePartialExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_INDEXSCAN: return new IndexScanExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_INDEXCOUNT: return new IndexCountExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_INSERT: return new InsertExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_INVALID:
        VOLT_ERROR("INVALID plan node type %d", (int) type);
        return NULL;
    case PLAN_NODE_TYPE_LIMIT: return new LimitExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_MATERIALIZE: return new MaterializeExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_MATERIALIZEDSCAN: return new MaterializedScanExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_MERGERECEIVE: return new MergeReceiveExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_NESTLOOP: return new NestLoopExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_NESTLOOPINDEX: return new NestLoopIndexExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_ORDERBY: return new OrderByExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_PROJECTION: return new ProjectionExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_RECEIVE: return new ReceiveExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_SEND: return new SendExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_SEQSCAN: return new SeqScanExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_TABLECOUNT: return new TableCountExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_TUPLESCAN: return new TupleScanExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_UNION: return new UnionExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_UPDATE: return new UpdateExecutor(engine, abstract_node);
    case PLAN_NODE_TYPE_WINDOWFUNCTION: return new WindowFunctionExecutor(engine, abstract_node);
    // default: Don't provide a default, let the compiler enforce complete coverage.
    }
    VOLT_ERROR("Undefined plan node type %d", (int) type);
    return NULL;
}

}
