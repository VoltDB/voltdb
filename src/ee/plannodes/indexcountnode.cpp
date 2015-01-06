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


#include "indexcountnode.h"

#include "expressions/abstractexpression.h"

#include <sstream>

namespace voltdb {

PlanNodeType IndexCountPlanNode::getPlanNodeType() const { return PLAN_NODE_TYPE_INDEXCOUNT; }

IndexCountPlanNode::~IndexCountPlanNode() { }

std::string IndexCountPlanNode::debugInfo(const std::string &spacer) const
{
    std::ostringstream buffer;
    buffer << AbstractScanPlanNode::debugInfo(spacer);
    buffer << spacer << "TargetIndexName[" << m_target_index_name << "]\n";
    buffer << spacer << "IndexLookupType[" << m_lookup_type << "]\n";

    buffer << spacer << "SearchKey Expressions:\n";
    for (int ctr = 0, cnt = (int)m_searchkey_expressions.size(); ctr < cnt; ctr++) {
        buffer << m_searchkey_expressions[ctr]->debug(spacer);
    }

    buffer << spacer << "EndKey Expressions:\n";
    for (int ctr = 0, cnt = (int)m_endkey_expressions.size(); ctr < cnt; ctr++) {
        buffer << m_endkey_expressions[ctr]->debug(spacer);
    }

    buffer << spacer << "Post-Scan Expression: ";
    if (m_predicate != NULL) {
        buffer << "\n" << m_predicate->debug(spacer);
    } else {
        buffer << "<NULL>\n";
    }
    return buffer.str();
}

void IndexCountPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
    AbstractScanPlanNode::loadFromJSONObject(obj);

    std::string endTypeString = obj.valueForKey("END_TYPE").asStr();
    m_end_type = stringToIndexLookup(endTypeString);

    std::string lookupTypeString = obj.valueForKey("LOOKUP_TYPE").asStr();
    m_lookup_type = stringToIndexLookup(lookupTypeString);

    m_target_index_name = obj.valueForKey("TARGET_INDEX_NAME").asStr();

    m_searchkey_expressions.loadExpressionArrayFromJSONObject("SEARCHKEY_EXPRESSIONS", obj);
    m_endkey_expressions.loadExpressionArrayFromJSONObject("ENDKEY_EXPRESSIONS", obj);

    m_skip_null_predicate.reset(loadExpressionFromJSONObject("SKIP_NULL_PREDICATE", obj));
}

} // namespace voltdb
