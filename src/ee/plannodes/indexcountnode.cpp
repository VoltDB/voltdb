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
    for (int ii = 0; ii < searchkey_expressions.size(); ii++) {
        delete searchkey_expressions[ii];
    }
    for (int ii = 0; ii < endkey_expressions.size(); ii++) {
        delete endkey_expressions[ii];
    }
    delete count_null_expression;
    delete getOutputTable();
    setOutputTable(NULL);
}

void IndexCountPlanNode::setLookupType(IndexLookupType lookup_type) {
    this->lookup_type = lookup_type;
}
IndexLookupType IndexCountPlanNode::getLookupType() const {
    return lookup_type;
}

void IndexCountPlanNode::setEndType(IndexLookupType end_type) {
    this->end_type = end_type;
}
IndexLookupType IndexCountPlanNode::getEndType() const {
    return end_type;
}

void IndexCountPlanNode::setTargetIndexName(std::string name) {
    this->target_index_name = name;
}
std::string IndexCountPlanNode::getTargetIndexName() const {
    return this->target_index_name;
}

void IndexCountPlanNode::setEndKeyEndExpressions(std::vector<AbstractExpression*> &exps) {
    this->endkey_expressions = exps;
}
std::vector<AbstractExpression*>& IndexCountPlanNode::getEndKeyExpressions() {
    return (this->endkey_expressions);
}
const std::vector<AbstractExpression*>& IndexCountPlanNode::getEndKeyExpressions() const {
    return (this->endkey_expressions);
}

void IndexCountPlanNode::setSearchKeyExpressions(std::vector<AbstractExpression*> &exps) {
    this->searchkey_expressions = exps;
}
std::vector<AbstractExpression*>& IndexCountPlanNode::getSearchKeyExpressions() {
    return (this->searchkey_expressions);
}
const std::vector<AbstractExpression*>& IndexCountPlanNode::getSearchKeyExpressions() const {
    return (this->searchkey_expressions);
}

AbstractExpression* IndexCountPlanNode::getCountNULLExpression() const {
    return (this->count_null_expression);
}

std::string IndexCountPlanNode::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << this->AbstractScanPlanNode::debugInfo(spacer);
    buffer << spacer << "TargetIndexName[" << this->target_index_name << "]\n";
    buffer << spacer << "EnableKeyIteration[" << std::boolalpha << this->key_iterate << "]\n";
    buffer << spacer << "IndexLookupType[" << this->lookup_type << "]\n";

    buffer << spacer << "SearchKey Expressions:\n";
    for (int ctr = 0, cnt = (int)this->searchkey_expressions.size(); ctr < cnt; ctr++) {
        buffer << this->searchkey_expressions[ctr]->debug(spacer);
    }

    buffer << spacer << "EndKey Expressions:\n";
    for (int ctr = 0, cnt = (int)this->endkey_expressions.size(); ctr < cnt; ctr++) {
        buffer << this->endkey_expressions[ctr]->debug(spacer);
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

    key_iterate = obj.valueForKey("KEY_ITERATE").asBool();

    std::string endTypeString = obj.valueForKey("END_TYPE").asStr();
    end_type = stringToIndexLookup(endTypeString);

    std::string lookupTypeString = obj.valueForKey("LOOKUP_TYPE").asStr();
    lookup_type = stringToIndexLookup(lookupTypeString);

    target_index_name = obj.valueForKey("TARGET_INDEX_NAME").asStr();

    if (obj.hasNonNullKey("ENDKEY_EXPRESSIONS")) {
        PlannerDomValue endKeyExprArray = obj.valueForKey("ENDKEY_EXPRESSIONS");
        for (int i = 0; i < endKeyExprArray.arrayLen(); i++) {
            AbstractExpression *expr = AbstractExpression::buildExpressionTree(endKeyExprArray.valueAtIndex(i));
            endkey_expressions.push_back(expr);
        }
    }
    else {
        endkey_expressions.clear();
    }

    PlannerDomValue searchKeyExprArray = obj.valueForKey("SEARCHKEY_EXPRESSIONS");
    for (int i = 0; i < searchKeyExprArray.arrayLen(); i++) {
        AbstractExpression *expr = AbstractExpression::buildExpressionTree(searchKeyExprArray.valueAtIndex(i));
        searchkey_expressions.push_back(expr);
    }

    if (obj.hasNonNullKey("COUNT_NULL_EXPRESSION")) {
        PlannerDomValue exprValue = obj.valueForKey("COUNT_NULL_EXPRESSION");
        count_null_expression = AbstractExpression::buildExpressionTree(exprValue);
    } else {
        count_null_expression = NULL;
    }
}

}
