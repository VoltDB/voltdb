/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
#include "rankscannode.h"

#include "common/debuglog.h"
#include "expressions/abstractexpression.h"
#include "expressions/rankexpression.h"

#include <sstream>

namespace voltdb {

RankScanPlanNode::~RankScanPlanNode() { }

PlanNodeType RankScanPlanNode::getPlanNodeType() const { return PLAN_NODE_TYPE_RANKSCAN; }

RankExpression* RankScanPlanNode::getRankExpression() const {
    RankExpression * rankExpr = dynamic_cast<RankExpression*> (m_rank_expression.get());
    return rankExpr;
}

std::string RankScanPlanNode::debugInfo(const std::string &spacer) const
{
    std::ostringstream buffer;
    buffer << AbstractScanPlanNode::debugInfo(spacer);

    buffer << spacer << "Rank Expression: ";
    if (m_rank_expression != NULL) {
        buffer << "\n" << m_rank_expression->debug(spacer);
    } else {
        buffer << "<NULL>\n";
    }

    buffer << spacer << "RankKey Expression:\n";
    if (m_rankkey_expression != NULL) {
        buffer << "\n" << m_rankkey_expression->debug(spacer);
    } else {
        buffer << "<NULL>\n";
    }

    buffer << spacer << "End Expression: ";
    if (m_end_expression != NULL) {
        buffer << "\n" << m_end_expression->debug(spacer);
    } else {
        buffer << "<NULL>\n";
    }

    buffer << spacer << "Post-Scan Expression: ";
    if (m_predicate != NULL) {
        buffer << "\n" << m_predicate->debug(spacer);
    } else {
        buffer << "<NULL>\n";
    }
    return buffer.str();
}

void RankScanPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
    AbstractScanPlanNode::loadFromJSONObject(obj);

    std::string lookupTypeString = obj.valueForKey("RANK_START_TYPE").asStr();
    m_lookupType = stringToIndexLookup(lookupTypeString);

    if (obj.hasKey("RANK_END_TYPE")) {
        lookupTypeString = obj.valueForKey("RANK_END_TYPE").asStr();
        m_endType = stringToIndexLookup(lookupTypeString);
        m_end_expression.reset(loadExpressionFromJSONObject("RANK_END_VALUE_EXPRESSION", obj));
    }

    m_rank_expression.reset(loadExpressionFromJSONObject("RANK_EXPRESSION", obj));
    m_rankkey_expression.reset(loadExpressionFromJSONObject("RANK_START_VALUE_EXPRESSION", obj));
}

} // namespace voltdb
