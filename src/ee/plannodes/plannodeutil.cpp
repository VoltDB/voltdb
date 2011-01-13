/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

#include <sstream>
#include "plannodes/plannodeutil.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/FatalException.hpp"
#include "plannodes/nodes.h"

namespace plannodeutil {

voltdb::AbstractPlanNode* getEmptyPlanNode(voltdb::PlanNodeType type) {
    VOLT_TRACE("Creating an empty PlanNode of type '%s'", plannodeutil::getTypeName(type).c_str());
    voltdb::AbstractPlanNode* ret = NULL;
    switch (type) {
        // ------------------------------------------------------------------
        // SeqScan
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_SEQSCAN):
            ret = new voltdb::SeqScanPlanNode();
            break;
        // ------------------------------------------------------------------
        // IndexScan
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_INDEXSCAN):
            ret = new voltdb::IndexScanPlanNode();
            break;
        // ------------------------------------------------------------------
        // NestLoop
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_NESTLOOP):
            ret = new voltdb::NestLoopPlanNode();
            break;
        // ------------------------------------------------------------------
        // NestLoopIndex
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_NESTLOOPINDEX):
            ret = new voltdb::NestLoopIndexPlanNode();
            break;
        // ------------------------------------------------------------------
        // Update
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_UPDATE):
            ret = new voltdb::UpdatePlanNode();
            break;
        // ------------------------------------------------------------------
        // Insert
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_INSERT):
            ret = new voltdb::InsertPlanNode();
            break;
        // ------------------------------------------------------------------
        // Delete
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_DELETE):
            ret = new voltdb::DeletePlanNode();
            break;
        // ------------------------------------------------------------------
        // Aggregate
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_HASHAGGREGATE):
        case (voltdb::PLAN_NODE_TYPE_AGGREGATE):
            ret = new voltdb::AggregatePlanNode(type);
            break;
        // ------------------------------------------------------------------
        // Union
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_UNION):
            ret = new voltdb::UnionPlanNode();
            break;
        // ------------------------------------------------------------------
        // OrderBy
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_ORDERBY):
            ret = new voltdb::OrderByPlanNode();
            break;
        // ------------------------------------------------------------------
        // Projection
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_PROJECTION):
            ret = new voltdb::ProjectionPlanNode();
            break;
        // ------------------------------------------------------------------
        // Materialize
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_MATERIALIZE):
            ret = new voltdb::MaterializePlanNode();
            break;
        // ------------------------------------------------------------------
        // Send
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_SEND):
            ret = new voltdb::SendPlanNode();
            break;
        // ------------------------------------------------------------------
        // Limit
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_LIMIT):
            ret = new voltdb::LimitPlanNode();
            break;
        // ------------------------------------------------------------------
        // Distinct
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_DISTINCT):
            ret = new voltdb::DistinctPlanNode();
            break;
        // ------------------------------------------------------------------
        // Receive
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_RECEIVE):
            ret = new voltdb::ReceivePlanNode();
            break;

        // ------------------------------------------------------------------
        // UNKNOWN
        // ------------------------------------------------------------------
        default: {
            throwFatalException("Invalid PlanNode type '%d'", type);
        }
    }
    //VOLT_TRACE("created plannode : %s ", typeid(*ret).name());
    return (ret);
}

std::string getTypeName(voltdb::PlanNodeType type) {
    std::string ret;
    switch (type) {
        // ------------------------------------------------------------------
        // SeqScan
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_SEQSCAN):
            ret = "SEQSCAN";
            break;
        // ------------------------------------------------------------------
        // IndexScan
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_INDEXSCAN):
            ret = "INDEXSCAN";
            break;
        // ------------------------------------------------------------------
        // NestLoop
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_NESTLOOP):
            ret = "NESTLOOP";
            break;
        // ------------------------------------------------------------------
        // NestLoopIndex
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_NESTLOOPINDEX):
            ret = "NESTLOOPINDEX";
            break;
        // ------------------------------------------------------------------
        // Update
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_UPDATE):
            ret = "UPDATE";
            break;
        // ------------------------------------------------------------------
        // Insert
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_INSERT):
            ret = "INSERT";
            break;
        // ------------------------------------------------------------------
        // Delete
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_DELETE):
            ret = "DELETE";
            break;
        // ------------------------------------------------------------------
        // Send
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_SEND):
            ret = "SEND";
            break;
        // ------------------------------------------------------------------
        // Receive
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_RECEIVE):
            ret = "RECEIVE";
            break;
        // ------------------------------------------------------------------
        // Aggregate
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_AGGREGATE):
            ret = "AGGREGATE";
            break;
        // ------------------------------------------------------------------
        // HashAggregate
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_HASHAGGREGATE):
            ret = "HASHAGGREGATE";
            break;
        // ------------------------------------------------------------------
        // Union
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_UNION):
            ret = "UNION";
            break;
        // ------------------------------------------------------------------
        // OrderBy
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_ORDERBY):
            ret = "ORDERBY";
            break;
        // ------------------------------------------------------------------
        // Projection
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_PROJECTION):
            ret = "PROJECTION";
            break;
        // ------------------------------------------------------------------
        // Materialize
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_MATERIALIZE):
            ret = "MATERIALIZE";
            break;
        // ------------------------------------------------------------------
        // Limit
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_LIMIT):
            ret = "LIMIT";
            break;
        // ------------------------------------------------------------------
        // Distinct
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_DISTINCT):
            ret = "DISTINCT";
            break;
        // ------------------------------------------------------------------
        // UNKNOWN
        // ------------------------------------------------------------------
        default: {
            throwFatalException( "Invalid PlanNode type '%d'", type);
        }
    }
    return (ret);
}

std::string debug(const voltdb::AbstractPlanNode* node) {
    //
    // TODO: This should display the entire plan tree
    // Use this algorithm: http://search.cpan.org/src/ISAACSON/Text-Tree-1.0/lib/Text/Tree.pm
    //
    assert(node != NULL);
    std::string spacer = "";
    return (plannodeutil::debug(node, spacer));
}

std::string debug(const voltdb::AbstractPlanNode* node, std::string spacer) {
    assert(node);
    std::ostringstream buffer;
    //VOLT_ERROR("%s", node->getId().debug().c_str());
    buffer <<  spacer << "->" << getTypeName(node->getPlanNodeType());
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

}
