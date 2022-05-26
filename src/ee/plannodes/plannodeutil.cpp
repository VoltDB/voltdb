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

#include "plannodes/plannodeutil.h"
#include "plannodes/aggregatenode.h"
#include "plannodes/deletenode.h"
#include "plannodes/migratenode.h"
#include "plannodes/indexscannode.h"
#include "plannodes/indexcountnode.h"
#include "plannodes/tablecountnode.h"
#include "plannodes/insertnode.h"
#include "plannodes/limitnode.h"
#include "plannodes/materializenode.h"
#include "plannodes/materializedscanplannode.h"
#include "plannodes/mergereceivenode.h"
#include "plannodes/mergejoinnode.h"
#include "plannodes/migratenode.h"
#include "plannodes/nestloopnode.h"
#include "plannodes/nestloopindexnode.h"
#include "plannodes/orderbynode.h"
#include "plannodes/receivenode.h"
#include "plannodes/commontablenode.h"
#include "plannodes/sendnode.h"
#include "plannodes/seqscannode.h"
#include "plannodes/swaptablesnode.h"
#include "plannodes/tuplescannode.h"
#include "plannodes/unionnode.h"
#include "plannodes/updatenode.h"
#include "plannodes/windowfunctionnode.h"

namespace voltdb {
namespace plannodeutil {

voltdb::AbstractPlanNode* getEmptyPlanNode(voltdb::PlanNodeType type) {
    VOLT_TRACE("Creating an empty PlanNode of type '%s'", planNodeToString(type).c_str());
    voltdb::AbstractPlanNode* ret = NULL;
    switch (type) {
        case (voltdb::PlanNodeType::Invalid):
            throwSerializableEEException("INVALID plan node type");
            break;
        // ------------------------------------------------------------------
        // SeqScan
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::SeqScan):
            ret = new voltdb::SeqScanPlanNode();
            break;
        // ------------------------------------------------------------------
        // IndexScan
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::IndexScan):
            ret = new voltdb::IndexScanPlanNode();
            break;
        // ------------------------------------------------------------------
        // IndexCount
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::IndexCount):
            ret = new voltdb::IndexCountPlanNode();
            break;
        // ------------------------------------------------------------------
        // TableCount
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::TableCount):
            ret = new voltdb::TableCountPlanNode();
            break;
        // ------------------------------------------------------------------
        // MaterializedScanPlanNode
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::MaterializedScan):
            ret = new voltdb::MaterializedScanPlanNode();
            break;
        // ------------------------------------------------------------------
        // TupleScanPlanNode
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::TupleScan):
            ret = new voltdb::TupleScanPlanNode();
            break;
        // ------------------------------------------------------------------
        // NestLoop
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::Nestloop):
            ret = new voltdb::NestLoopPlanNode();
            break;
        // ------------------------------------------------------------------
        // NestLoopIndex
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::NestloopIndex):
            ret = new voltdb::NestLoopIndexPlanNode();
            break;
        // ------------------------------------------------------------------
        // MergeJoin
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::MergeJoin):
            ret = new voltdb::MergeJoinPlanNode();
            break;
        // ------------------------------------------------------------------
        // Update
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::Update):
            ret = new voltdb::UpdatePlanNode();
            break;
        // ------------------------------------------------------------------
        // Insert
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::Insert):
            ret = new voltdb::InsertPlanNode();
            break;
        // ------------------------------------------------------------------
        // Delete
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::Delete):
            ret = new voltdb::DeletePlanNode();
            break;
        // ------------------------------------------------------------------
        // Migrate
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::Migrate):
            ret = new voltdb::MigratePlanNode();
            break;
        // ------------------------------------------------------------------
        // SwapTables
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::SwapTables):
            ret = new voltdb::SwapTablesPlanNode();
            break;
        // ------------------------------------------------------------------
        // Aggregate
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::HashAggregate):
        case (voltdb::PlanNodeType::Aggregate):
        case (voltdb::PlanNodeType::PartialAggregate):
            ret = new voltdb::AggregatePlanNode(type);
            break;
        // ------------------------------------------------------------------
        // Union
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::Union):
            ret = new voltdb::UnionPlanNode();
            break;
        // ------------------------------------------------------------------
        // OrderBy
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::OrderBy):
            ret = new voltdb::OrderByPlanNode();
            break;
        // ------------------------------------------------------------------
        // Projection
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::Projection):
            ret = new voltdb::ProjectionPlanNode();
            break;
        // ------------------------------------------------------------------
        // Materialize
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::Materialize):
            ret = new voltdb::MaterializePlanNode();
            break;
        // ------------------------------------------------------------------
        // Send
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::Send):
            ret = new voltdb::SendPlanNode();
            break;
        // ------------------------------------------------------------------
        // Limit
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::Limit):
            ret = new voltdb::LimitPlanNode();
            break;
        // ------------------------------------------------------------------
        // Receive
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::Receive):
            ret = new voltdb::ReceivePlanNode();
            break;
        // ------------------------------------------------------------------
        // Merge Receive
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::MergeReceive):
            ret = new voltdb::MergeReceivePlanNode();
            break;
        // ------------------------------------------------------------------
        // Window Function
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::WindowFunction):
            ret = new voltdb::WindowFunctionPlanNode();
            break;
        // ------------------------------------------------------------------
        // Common Table
        // ------------------------------------------------------------------
        case (voltdb::PlanNodeType::CommonTable):
            ret = new voltdb::CommonTablePlanNode();
            break;
        // default: Don't provide a default, let the compiler enforce complete coverage.
    }

    // ------------------------------------------------------------------
    // UNKNOWN
    // ------------------------------------------------------------------
    if (!ret) {
        throwFatalException("Undefined plan node type '%d'", (int)type);
    }
    //VOLT_TRACE("created plannode : %s ", typeid(*ret).name());
    return (ret);
}

std::string debug(const voltdb::AbstractPlanNode* node) {
    //
    // TODO: This should display the entire plan tree
    // Use this algorithm: http://search.cpan.org/src/ISAACSON/Text-Tree-1.0/lib/Text/Tree.pm
    //
    vassert(node != NULL);
    std::string spacer = "";
    return (plannodeutil::debug(node, spacer));
}

std::string debug(const voltdb::AbstractPlanNode* node, std::string spacer) {
    vassert(node);
    std::ostringstream buffer;
    //VOLT_ERROR("%s", node->getId().debug().c_str());
    buffer <<  spacer << "->" << planNodeToString(node->getPlanNodeType());
    buffer << "[" << node->getPlanNodeId() << "]:\n";
    //VOLT_ERROR("%s", buffer.str().c_str());

    spacer += "  ";
    if (!node->getChildren().empty()) {
        std::vector<voltdb::AbstractPlanNode*>::const_iterator child;
        for (child = node->getChildren().begin(); child != node->getChildren().end(); ++child) {
            buffer << plannodeutil::debug((*child), spacer);
        }
    }
    return (buffer.str());
}

} // end namespace plannodeutil
} // end namespace voltdb
