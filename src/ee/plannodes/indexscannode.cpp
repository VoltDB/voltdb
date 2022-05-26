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
#include "indexscannode.h"

#include <sstream>

namespace voltdb {

IndexScanPlanNode::~IndexScanPlanNode() { }

PlanNodeType IndexScanPlanNode::getPlanNodeType() const {
    return PlanNodeType::IndexScan;
}

std::string IndexScanPlanNode::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << AbstractScanPlanNode::debugInfo(spacer);
    buffer << spacer << "TargetIndexName[" << m_target_index_name << "]\n";
    buffer << spacer << "IndexLookupType["
           << indexLookupToString(m_lookup_type) << "]\n";
    buffer << spacer << "SortDirection["
           << sortDirectionToString(m_sort_direction) << "]\n";

    buffer << spacer << "SearchKey Expressions:\n";
    for (int ctr = 0, cnt = (int)m_searchkey_expressions.size(); ctr < cnt; ctr++) {
        buffer << m_searchkey_expressions[ctr]->debug(spacer);
    }

    buffer << spacer << "Ignore null candidate value flags for search keys:\n";
    for (int ctr = 0, cnt = (int)m_compare_not_distinct.size(); ctr < cnt; ctr++) {
        buffer << spacer << (m_compare_not_distinct[ctr] ? "true" : "false");
    }

    buffer << spacer << "End Expression: ";
    if (m_end_expression != NULL) {
        buffer << "\n" << m_end_expression->debug(spacer);
    } else {
        buffer << "<NULL>\n";
    }

    buffer << spacer << "Skip Null Expression: ";
    if (m_skip_null_predicate != NULL) {
        buffer << "\n" << m_skip_null_predicate->debug(spacer);
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

void IndexScanPlanNode::loadFromJSONObject(PlannerDomValue obj) {
    AbstractScanPlanNode::loadFromJSONObject(obj);

    std::string lookupTypeString = obj.valueForKey("LOOKUP_TYPE").asStr();
    m_lookup_type = stringToIndexLookup(lookupTypeString);

    if (obj.hasKey("HAS_OFFSET_RANK")) {
        m_hasOffsetRank = obj.valueForKey("HAS_OFFSET_RANK").asBool();
    }

    std::string sortDirectionString = obj.valueForKey("SORT_DIRECTION").asStr();
    m_sort_direction = stringToSortDirection(sortDirectionString);

    m_target_index_name = obj.valueForKey("TARGET_INDEX_NAME").asStr();

    m_end_expression.reset(loadExpressionFromJSONObject("END_EXPRESSION", obj));
    m_initial_expression.reset(loadExpressionFromJSONObject("INITIAL_EXPRESSION", obj));
    m_skip_null_predicate.reset(loadExpressionFromJSONObject("SKIP_NULL_PREDICATE", obj));

    m_searchkey_expressions.loadExpressionArrayFromJSONObject("SEARCHKEY_EXPRESSIONS", obj);
    loadBooleanArrayFromJSONObject("COMPARE_NOTDISTINCT", obj, m_compare_not_distinct);
}

} // namespace voltdb
