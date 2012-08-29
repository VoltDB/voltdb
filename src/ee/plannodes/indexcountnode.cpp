/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

void IndexCountPlanNode::loadFromJSONObject(json_spirit::Object &obj) {
    AbstractScanPlanNode::loadFromJSONObject(obj);
    json_spirit::Value keyIterateValue = json_spirit::find_value( obj, "KEY_ITERATE");
    if (keyIterateValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexCountPlanNode::loadFromJSONObject:"
                                      " Can't find KEY_ITERATE value");
    }
    key_iterate = keyIterateValue.get_bool();

    json_spirit::Value endTypeValue = json_spirit::find_value( obj, "END_TYPE");
    if (endTypeValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                "IndexCountPlanNode::loadFromJSONObject:"
                " Can't find END_TYPE");
    }
    std::string endTypeString = endTypeValue.get_str();
    end_type = stringToIndexLookup(endTypeString);

    json_spirit::Value lookupTypeValue = json_spirit::find_value( obj, "LOOKUP_TYPE");
    if (lookupTypeValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexCountPlanNode::loadFromJSONObject:"
                                      " Can't find LOOKUP_TYPE");
    }
    std::string lookupTypeString = lookupTypeValue.get_str();
    lookup_type = stringToIndexLookup(lookupTypeString);

    json_spirit::Value targetIndexNameValue = json_spirit::find_value( obj, "TARGET_INDEX_NAME");
    if (targetIndexNameValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexCountPlanNode::loadFromJSONObject:"
                                      " Can't find TARGET_INDEX_NAME");
    }
    target_index_name = targetIndexNameValue.get_str();

    json_spirit::Value endKeyExpressionsValue = json_spirit::find_value( obj, "ENDKEY_EXPRESSIONS");
    if (endKeyExpressionsValue == json_spirit::Value::null) {
        endkey_expressions.clear();
    } else {
        json_spirit::Array endKeyExpressionsArray = endKeyExpressionsValue.get_array();
        for (int ii = 0; ii < endKeyExpressionsArray.size(); ii++) {
            json_spirit::Object endKeyExpressionObject = endKeyExpressionsArray[ii].get_obj();
            AbstractExpression *expr = AbstractExpression::buildExpressionTree(endKeyExpressionObject);
            endkey_expressions.push_back(expr);
        }
    }


    json_spirit::Value searchKeyExpressionsValue = json_spirit::find_value( obj, "SEARCHKEY_EXPRESSIONS");
    if (searchKeyExpressionsValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "IndexCountPlanNode::loadFromJSONObject:"
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
