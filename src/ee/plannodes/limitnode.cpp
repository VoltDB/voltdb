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

/*
 * This code is needed in the limit executor as well as anywhere limit
 * is inlined. Centralize it here.
 */
void
LimitPlanNode::getLimitAndOffsetByReference(const NValueArray &params, int &limitOut, int &offsetOut)
{
    limitOut = limit;
    offsetOut = offset;

    // Limit and offset parameters strictly integers. Can't limit <?=varchar>.
    // Converting the loop counter to NValue's doesn't make it cleaner -
    // and probably makes it slower. Would have to initialize an nvalue for
    // each loop iteration.
    if (limitParamIdx != -1) {
        limitOut = ValuePeeker::peekInteger(params[limitParamIdx]);
        if (limitOut < 0) {
            throw SQLException(SQLException::data_exception_invalid_parameter,
                               "Negative parameter to LIMIT");
        }

    }
    if (offsetParamIdx != -1) {
        offsetOut = ValuePeeker::peekInteger(params[offsetParamIdx]);
        if (offsetOut < 0) {
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
        limitOut = ValuePeeker::peekAsInteger(limitExpression->eval(NULL, NULL));
        assert(offsetOut == 0);
    }
}

std::string LimitPlanNode::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << spacer << "Limit[" << this->limit << "]\n";
    buffer << spacer << "Offset[" << this->offset << "]\n";
    return (buffer.str());
}

void LimitPlanNode::loadFromJSONObject(PlannerDomValue obj) {
    limit = obj.valueForKey("LIMIT").asInt();
    offset = obj.valueForKey("OFFSET").asInt();

    if (obj.hasNonNullKey("LIMIT_PARAM_IDX")) {
        limitParamIdx = obj.valueForKey("LIMIT_PARAM_IDX").asInt();
    }
    if (obj.hasNonNullKey("OFFSET_PARAM_IDX")) {
        offsetParamIdx = obj.valueForKey("OFFSET_PARAM_IDX").asInt();
    }

    if (obj.hasNonNullKey("LIMIT_EXPRESSION")) {
        PlannerDomValue expr = obj.valueForKey("LIMIT_EXPRESSION");
        limitExpression = AbstractExpression::buildExpressionTree(expr);
    }
    else {
        limitExpression = NULL;
    }
}

}
