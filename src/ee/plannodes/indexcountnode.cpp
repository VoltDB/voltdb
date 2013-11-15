/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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


#include <stdexcept>
#include <sstream>
#include "indexcountnode.h"
#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/types.h"
#include "common/FatalException.hpp"
#include "expressions/abstractexpression.h"
#include "catalog/table.h"
#include "catalog/index.h"
#include "storage/table.h"

namespace voltdb {

IndexCountPlanNode::~IndexCountPlanNode() {
    for (int ii = 0; ii < m_searchkey_expressions.size(); ii++) {
        delete m_searchkey_expressions[ii];
    }
    for (int ii = 0; ii < m_endkey_expressions.size(); ii++) {
        delete m_endkey_expressions[ii];
    }
    delete m_count_null_expression;
    delete getOutputTable();
    setOutputTable(NULL);
}

std::string IndexCountPlanNode::debugInfo(const std::string &spacer) const {
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
    return (buffer.str());
}

void IndexCountPlanNode::loadFromJSONObject(PlannerDomValue obj) {
    AbstractScanPlanNode::loadFromJSONObject(obj);

    std::string endTypeString = obj.valueForKey("END_TYPE").asStr();
    m_end_type = stringToIndexLookup(endTypeString);

    std::string lookupTypeString = obj.valueForKey("LOOKUP_TYPE").asStr();
    m_lookup_type = stringToIndexLookup(lookupTypeString);

    m_target_index_name = obj.valueForKey("TARGET_INDEX_NAME").asStr();

    if (obj.hasNonNullKey("ENDKEY_EXPRESSIONS")) {
        PlannerDomValue endKeyExprArray = obj.valueForKey("ENDKEY_EXPRESSIONS");
        for (int i = 0; i < endKeyExprArray.arrayLen(); i++) {
            AbstractExpression *expr =
                AbstractExpression::buildExpressionTree(endKeyExprArray.valueAtIndex(i));
            m_endkey_expressions.push_back(expr);
        }
    }

    PlannerDomValue searchKeyExprArray = obj.valueForKey("SEARCHKEY_EXPRESSIONS");
    for (int i = 0; i < searchKeyExprArray.arrayLen(); i++) {
        AbstractExpression *expr =
            AbstractExpression::buildExpressionTree(searchKeyExprArray.valueAtIndex(i));
        m_searchkey_expressions.push_back(expr);
    }

    if (obj.hasNonNullKey("COUNT_NULL_EXPRESSION")) {
        PlannerDomValue exprValue = obj.valueForKey("COUNT_NULL_EXPRESSION");
        m_count_null_expression = AbstractExpression::buildExpressionTree(exprValue);
    }
}

}
