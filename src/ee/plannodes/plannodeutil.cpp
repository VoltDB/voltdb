/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#include "plannodes/plannodeutil.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/FatalException.hpp"
#include "plannodes/aggregatenode.h"
#include "plannodes/deletenode.h"
#include "plannodes/indexscannode.h"
#include "plannodes/indexcountnode.h"
#include "plannodes/tablecountnode.h"
#include "plannodes/insertnode.h"
#include "plannodes/limitnode.h"
#include "plannodes/materializenode.h"
#include "plannodes/materializedscanplannode.h"
#include "plannodes/nestloopnode.h"
#include "plannodes/nestloopindexnode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/orderbynode.h"
#include "plannodes/receivenode.h"
#include "plannodes/sendnode.h"
#include "plannodes/seqscannode.h"
#include "plannodes/unionnode.h"
#include "plannodes/updatenode.h"

#include <sstream>

namespace plannodeutil {

voltdb::AbstractPlanNode* getEmptyPlanNode(voltdb::PlanNodeType type) {
    VOLT_TRACE("Creating an empty PlanNode of type '%s'", planNodeToString(type).c_str());
    voltdb::AbstractPlanNode* ret = NULL;
    switch (type) {
        case (voltdb::PLAN_NODE_TYPE_INVALID): {
            throwFatalException("INVALID plan node type");
        }
            break;
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
        // IndexCount
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_INDEXCOUNT):
            ret = new voltdb::IndexCountPlanNode();
            break;
        // ------------------------------------------------------------------
        // TableCount
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_TABLECOUNT):
            ret = new voltdb::TableCountPlanNode();
            break;
        // ------------------------------------------------------------------
        // MaterializedScanPlanNode
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_MATERIALIZEDSCAN):
            ret = new voltdb::MaterializedScanPlanNode();
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
        case (voltdb::PLAN_NODE_TYPE_PARTIALAGGREGATE):
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
        // Receive
        // ------------------------------------------------------------------
        case (voltdb::PLAN_NODE_TYPE_RECEIVE):
            ret = new voltdb::ReceivePlanNode();
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
    assert(node != NULL);
    std::string spacer = "";
    return (plannodeutil::debug(node, spacer));
}

std::string debug(const voltdb::AbstractPlanNode* node, std::string spacer) {
    assert(node);
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

}
