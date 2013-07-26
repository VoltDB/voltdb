/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#include <stdexcept>
#include <sstream>
#include "indexscannode.h"
#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/types.h"
#include "common/FatalException.hpp"
#include "expressions/abstractexpression.h"
#include "catalog/table.h"
#include "catalog/index.h"
#include "storage/table.h"

namespace voltdb {

IndexScanPlanNode::~IndexScanPlanNode() {
    for (int ii = 0; ii < searchkey_expressions.size(); ii++) {
        delete searchkey_expressions[ii];
    }
    delete end_expression;
    delete initial_expression;
    delete getOutputTable();
    setOutputTable(NULL);
}

void IndexScanPlanNode::setKeyIterate(bool val) {
    this->key_iterate = val;
}
bool IndexScanPlanNode::getKeyIterate() const {
    return (this->key_iterate);
}

void IndexScanPlanNode::setLookupType(IndexLookupType lookup_type) {
    this->lookup_type = lookup_type;
}
IndexLookupType IndexScanPlanNode::getLookupType() const {
    return lookup_type;
}

void IndexScanPlanNode::setSortDirection(SortDirectionType val) {
    this->sort_direction = val;
}
SortDirectionType IndexScanPlanNode::getSortDirection() const {
    return sort_direction;
}

void IndexScanPlanNode::setTargetIndexName(std::string name) {
    this->target_index_name = name;
}
std::string IndexScanPlanNode::getTargetIndexName() const {
    return this->target_index_name;
}

void IndexScanPlanNode::setEndExpression(AbstractExpression* val) {
    // only expect this to be initialized once
    if (end_expression && end_expression != val)
    {
        throwFatalException("end_expression initialized twice in IndexScanPlanNode?");
        delete end_expression;
    }
    this->end_expression = val;
}
AbstractExpression* IndexScanPlanNode::getEndExpression() const {
    return (this->end_expression);
}

void IndexScanPlanNode::setSearchKeyExpressions(std::vector<AbstractExpression*> &exps) {
    this->searchkey_expressions = exps;
}
std::vector<AbstractExpression*>& IndexScanPlanNode::getSearchKeyExpressions() {
    return (this->searchkey_expressions);
}
const std::vector<AbstractExpression*>& IndexScanPlanNode::getSearchKeyExpressions() const {
    return (this->searchkey_expressions);
}

void IndexScanPlanNode::setInitialExpression(AbstractExpression* val) {
    // only expect this to be initialized once
    if (initial_expression && initial_expression != val)
    {
        throwFatalException("initial_expression initialized twice in IndexScanPlanNode?");
        delete initial_expression;
    }
    this->initial_expression = val;
}
AbstractExpression* IndexScanPlanNode::getInitialExpression() const {
    return (this->initial_expression);
}

std::string IndexScanPlanNode::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << this->AbstractScanPlanNode::debugInfo(spacer);
    buffer << spacer << "TargetIndexName[" << this->target_index_name << "]\n";
    buffer << spacer << "EnableKeyIteration[" << std::boolalpha << this->key_iterate << "]\n";
    buffer << spacer << "IndexLookupType[" << this->lookup_type << "]\n";
    buffer << spacer << "SortDirection[" << this->sort_direction << "]\n";

    buffer << spacer << "SearchKey Expressions:\n";
    for (int ctr = 0, cnt = (int)this->searchkey_expressions.size(); ctr < cnt; ctr++) {
        buffer << this->searchkey_expressions[ctr]->debug(spacer);
    }

    buffer << spacer << "End Expression: ";
    if (this->end_expression != NULL) {
        buffer << "\n" << this->end_expression->debug(spacer);
    } else {
        buffer << "<NULL>\n";
    }

    buffer << spacer << "Post-Scan Expression: ";
    if (m_predicate != NULL) {
        buffer << "\n" << m_predicate->debug(spacer);
    } else {
        buffer << "<NULL>\n";
    }
    return (buffer.str());
}

void IndexScanPlanNode::loadFromJSONObject(PlannerDomValue obj) {
    AbstractScanPlanNode::loadFromJSONObject(obj);

    key_iterate = obj.valueForKey("KEY_ITERATE").asBool();

    std::string lookupTypeString = obj.valueForKey("LOOKUP_TYPE").asStr();
    lookup_type = stringToIndexLookup(lookupTypeString);

    std::string sortDirectionString = obj.valueForKey("SORT_DIRECTION").asStr();
    sort_direction = stringToSortDirection(sortDirectionString);

    target_index_name = obj.valueForKey("TARGET_INDEX_NAME").asStr();

    if (obj.hasNonNullKey("END_EXPRESSION")) {
        PlannerDomValue exprValue = obj.valueForKey("END_EXPRESSION");
        end_expression = AbstractExpression::buildExpressionTree(exprValue);
    }
    else {
        end_expression = NULL;
    }

    if (obj.hasNonNullKey("INITIAL_EXPRESSION")) {
        PlannerDomValue exprValue = obj.valueForKey("INITIAL_EXPRESSION");
        initial_expression = AbstractExpression::buildExpressionTree(exprValue);
    } else {
        initial_expression = NULL;
    }

    PlannerDomValue searchKeyExprArray = obj.valueForKey("SEARCHKEY_EXPRESSIONS");
    for (int i = 0; i < searchKeyExprArray.arrayLen(); i++) {
        AbstractExpression *expr = AbstractExpression::buildExpressionTree(searchKeyExprArray.valueAtIndex(i));
        searchkey_expressions.push_back(expr);
    }
}

}
