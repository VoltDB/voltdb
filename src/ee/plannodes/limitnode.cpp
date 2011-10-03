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
#include <stdexcept>
#include "limitnode.h"
#include "common/serializeio.h"
#include "common/ValuePeeker.hpp"
#include "common/FatalException.hpp"
#include "storage/table.h"

namespace voltdb {

LimitPlanNode::~LimitPlanNode() {
    delete limitExpression;
    if (!isInline()) {
        delete getOutputTable();
        setOutputTable(NULL);
    }
}

void LimitPlanNode::setLimit(int limit) {
    this->limit = limit;
}
int LimitPlanNode::getLimit() const {
    return (this->limit);
}

void LimitPlanNode::setOffset(int offset) {
    this->offset = offset;
}
int LimitPlanNode::getOffset() const {
    return (this->offset);
}

AbstractExpression* LimitPlanNode::getLimitExpression() const {
    return this->limitExpression;
}

void LimitPlanNode::setLimitExpression(AbstractExpression* expression) {
    if (limitExpression && limitExpression != expression)
    {
        throwFatalException("limitExpression initialized twice in LimitPlanNode");
        delete limitExpression;
    }
    this->limitExpression = expression;
}

/*
 * This code is needed in the limit executor as well as anywhere limit
 * is inlined. Centralize it here.
 */
void
LimitPlanNode::getLimitAndOffsetByReference(const NValueArray &params, int &limit, int &offset)
{
    limit = getLimit();
    offset = getOffset();

    // Limit and offset parameters strictly integers. Can't limit <?=varchar>.
    // Converting the loop counter to NValue's doesn't make it cleaner -
    // and probably makes it slower. Would have to initialize an nvalue for
    // each loop iteration.
    if (getLimitParamIdx() != -1) {
        limit = ValuePeeker::peekInteger(params[getLimitParamIdx()]);
        if (limit < 0) {
            throw SQLException(SQLException::data_exception_invalid_parameter,
                               "Negative parameter to LIMIT");
        }

    }
    if (getOffsetParamIdx() != -1) {
        offset = ValuePeeker::peekInteger(params[getOffsetParamIdx()]);
        if (offset < 0) {
            throw SQLException(SQLException::data_exception_invalid_parameter,
                               "Negative parameter to LIMIT OFFSET");
        }
    }

    // If the limit expression is not null, we need to evaluate it and assign
    // the result to limit, offset must be 0
    if (limitExpression != NULL) {
        // The expression should be an operator expression with either constant
        // value expression or parameter value expression as children
        limitExpression->substitute(params);
        limit = ValuePeeker::peekAsInteger(limitExpression->eval(NULL, NULL));
        assert(offset == 0);
    }
}

std::string LimitPlanNode::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << spacer << "Limit[" << this->limit << "]\n";
    buffer << spacer << "Offset[" << this->offset << "]\n";
    return (buffer.str());
}

void LimitPlanNode::loadFromJSONObject(json_spirit::Object &obj) {
    json_spirit::Value limitValue = json_spirit::find_value( obj, "LIMIT");
    if (limitValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "LimitPlanNode::loadFromJSONObject:"
                                      " can't find LIMIT value");
    }
    limit = limitValue.get_int();

    json_spirit::Value offsetValue = json_spirit::find_value( obj, "OFFSET");
    if (offsetValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "LimitPlanNode::loadFromJSONObject:"
                                      " can't find OFFSET value");
    }
    offset = offsetValue.get_int();

    json_spirit::Value paramIdx = json_spirit::find_value(obj, "LIMIT_PARAM_IDX");
    if (!(paramIdx == json_spirit::Value::null)) {
        setLimitParamIdx(paramIdx.get_int());
    }
    paramIdx = json_spirit::find_value(obj, "OFFSET_PARAM_IDX");
    if (!(paramIdx == json_spirit::Value::null)) {
        setOffsetParamIdx(paramIdx.get_int());
    }

    json_spirit::Value expr = json_spirit::find_value(obj, "LIMIT_EXPRESSION");
    if (expr == json_spirit::Value::null) {
        limitExpression = NULL;
    } else {
        limitExpression = AbstractExpression::buildExpressionTree(expr.get_obj());
    }
}

}
