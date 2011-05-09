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

void IndexScanPlanNode::loadFromJSONObject(json_spirit::Object &obj) {
    AbstractScanPlanNode::loadFromJSONObject(obj);

    json_spirit::Value keyIterateValue = json_spirit::find_value( obj, "KEY_ITERATE");
    if (keyIterateValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexScanPlanNode::loadFromJSONObject:"
                                      " Can't find KEY_ITERATE value");
    }
    key_iterate = keyIterateValue.get_bool();

    json_spirit::Value lookupTypeValue = json_spirit::find_value( obj, "LOOKUP_TYPE");
    if (lookupTypeValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexScanPlanNode::loadFromJSONObject:"
                                      " Can't find LOOKUP_TYPE");
    }
    std::string lookupTypeString = lookupTypeValue.get_str();
    lookup_type = stringToIndexLookup(lookupTypeString);

    json_spirit::Value sortDirectionValue = json_spirit::find_value( obj, "SORT_DIRECTION");
    if (sortDirectionValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexScanPlanNode::loadFromJSONObject:"
                                      " Can't find SORT_DIRECTION");
    }
    std::string sortDirectionString = sortDirectionValue.get_str();
    sort_direction = stringToSortDirection(sortDirectionString);

    json_spirit::Value targetIndexNameValue = json_spirit::find_value( obj, "TARGET_INDEX_NAME");
    if (targetIndexNameValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexScanPlanNode::loadFromJSONObject:"
                                      " Can't find TARGET_INDEX_NAME");
    }
    target_index_name = targetIndexNameValue.get_str();

    json_spirit::Value endExpressionValue = json_spirit::find_value( obj, "END_EXPRESSION");
    if (endExpressionValue == json_spirit::Value::null) {
        end_expression = NULL;
    } else {
        end_expression = AbstractExpression::buildExpressionTree(endExpressionValue.get_obj());
    }

    json_spirit::Value searchKeyExpressionsValue = json_spirit::find_value( obj, "SEARCHKEY_EXPRESSIONS");
    if (searchKeyExpressionsValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexScanPlanNode::loadFromJSONObject:"
                                      " Can't find SEARCHKEY_EXPRESSIONS");
    }
    json_spirit::Array searchKeyExpressionsArray = searchKeyExpressionsValue.get_array();

    for (int ii = 0; ii < searchKeyExpressionsArray.size(); ii++) {
        json_spirit::Object searchKeyExpressionObject = searchKeyExpressionsArray[ii].get_obj();
        AbstractExpression *expr = AbstractExpression::buildExpressionTree(searchKeyExpressionObject);
        searchkey_expressions.push_back(expr);
    }
}

}
