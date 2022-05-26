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

#include "executorfactory.h"

#include "common/debuglog.h"
#include "common/FatalException.hpp"

#include "executors/abstractexecutor.h"
#include "executors/aggregateexecutor.h"
#include "executors/deleteexecutor.h"
#include "executors/migrateexecutor.h"
#include "executors/indexscanexecutor.h"
#include "executors/indexcountexecutor.h"
#include "executors/tablecountexecutor.h"
#include "executors/insertexecutor.h"
#include "executors/largeorderbyexecutor.h"
#include "executors/limitexecutor.h"
#include "executors/materializeexecutor.h"
#include "executors/materializedscanexecutor.h"
#include "executors/mergereceiveexecutor.h"
#include "executors/mergejoinexecutor.h"
#include "executors/migrateexecutor.h"
#include "executors/nestloopexecutor.h"
#include "executors/nestloopindexexecutor.h"
#include "executors/orderbyexecutor.h"
#include "executors/projectionexecutor.h"
#include "executors/receiveexecutor.h"
#include "executors/commontableexecutor.h"
#include "executors/sendexecutor.h"
#include "executors/seqscanexecutor.h"
#include "executors/swaptablesexecutor.h"
#include "executors/tuplescanexecutor.h"
#include "executors/unionexecutor.h"
#include "executors/updateexecutor.h"
#include "executors/windowfunctionexecutor.h"

namespace voltdb {

AbstractExecutor* getNewExecutor(
      VoltDBEngine *engine, AbstractPlanNode* abstract_node, bool isLargeQuery) {
   PlanNodeType type = abstract_node->getPlanNodeType();
   switch (type) {
      case PlanNodeType::Aggregate:
         return new AggregateSerialExecutor(engine, abstract_node);
      case PlanNodeType::Delete:
         return new DeleteExecutor(engine, abstract_node);
      case PlanNodeType::HashAggregate:
         return new AggregateHashExecutor(engine, abstract_node);
      case PlanNodeType::PartialAggregate:
         return new AggregatePartialExecutor(engine, abstract_node);
      case PlanNodeType::IndexScan:
         return new IndexScanExecutor(engine, abstract_node);
      case PlanNodeType::IndexCount:
         return new IndexCountExecutor(engine, abstract_node);
      case PlanNodeType::Insert:
         return new InsertExecutor(engine, abstract_node);
      case PlanNodeType::Migrate:
         return new MigrateExecutor(engine, abstract_node);
      case PlanNodeType::Limit:
         return new LimitExecutor(engine, abstract_node);
      case PlanNodeType::Materialize:
         return new MaterializeExecutor(engine, abstract_node);
      case PlanNodeType::MaterializedScan:
         return new MaterializedScanExecutor(engine, abstract_node);
      case PlanNodeType::MergeReceive:
         return new MergeReceiveExecutor(engine, abstract_node);
      case PlanNodeType::Nestloop:
         return new NestLoopExecutor(engine, abstract_node);
      case PlanNodeType::NestloopIndex:
         return new NestLoopIndexExecutor(engine, abstract_node);
      case PlanNodeType::MergeJoin:
         return new MergeJoinExecutor(engine, abstract_node);
      case PlanNodeType::OrderBy:
         if (isLargeQuery) {
            return new LargeOrderByExecutor(engine, abstract_node);
         } else {
            return new OrderByExecutor(engine, abstract_node);
         }
      case PlanNodeType::Projection:
         return new ProjectionExecutor(engine, abstract_node);
      case PlanNodeType::Receive:
         return new ReceiveExecutor(engine, abstract_node);
      case PlanNodeType::CommonTable:
         return new CommonTableExecutor(engine, abstract_node);
      case PlanNodeType::Send:
         return new SendExecutor(engine, abstract_node);
      case PlanNodeType::SeqScan:
         return new SeqScanExecutor(engine, abstract_node);
      case PlanNodeType::SwapTables:
         return new SwapTablesExecutor(engine, abstract_node);
      case PlanNodeType::TableCount:
         return new TableCountExecutor(engine, abstract_node);
      case PlanNodeType::TupleScan:
         return new TupleScanExecutor(engine, abstract_node);
      case PlanNodeType::Union:
         return new UnionExecutor(engine, abstract_node);
      case PlanNodeType::Update:
         return new UpdateExecutor(engine, abstract_node);
      case PlanNodeType::WindowFunction:
         return new WindowFunctionExecutor(engine, abstract_node);
      case PlanNodeType::Invalid:
         VOLT_ERROR("INVALID plan node type %d", (int) PlanNodeType::Invalid);
         return NULL;
         // default: Don't provide a default, let the compiler enforce complete coverage.
   }
   VOLT_ERROR("Undefined plan node type %d", (int) type);
   return NULL;
}

}
