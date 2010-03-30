/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
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

#include "incomparisonexpression.h"

#include "common/serializeio.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/FatalException.hpp"
#include "expressions/expressionutil.h"

#include <stdint.h>
#include <sstream>
#include <cassert>

namespace voltdb {

InComparisonExpression::InComparisonExpression(AbstractExpression *left,
                                               const std::vector<AbstractExpression*> &values)
  : AbstractExpression(EXPRESSION_TYPE_COMPARE_IN)
{
    this->m_left = left;
    // TODO: a copy of containers containing non-refcounted pointers. ugh.
    this->m_values = values;
}

InComparisonExpression::~InComparisonExpression()
{
    // TODO: delete m_values (which don't need to be a vector)
    for (int ii = 0; ii < m_values.size(); ii++) {
        delete m_values[ii];
    }
}

std::vector<AbstractExpression*>&
InComparisonExpression::getValueExpressions()
{
    return (this->m_values);
}

const std::vector<AbstractExpression*>&
InComparisonExpression::getValueExpressions() const
{
    return (this->m_values);
}

NValue
InComparisonExpression::eval(const TableTuple *tuple1,
                             const TableTuple *tuple2) const
{
    if (this->m_type != EXPRESSION_TYPE_COMPARE_IN) {
        char message[128];
        sprintf(message, "Invalid ExpressionType '%s' called for"
                " InComparisonExpression",
                expressionutil::getTypeName(this->m_type).c_str());
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_SQL,
                                      message);
    }

    // First get the left value
    const voltdb::NValue left_value = this->m_left->eval(tuple1, tuple2);

    // Then iterate m_values looking for a match, stopping if one is found.
    const size_t num_of_values = this->m_values.size();
    assert(num_of_values > 0);
    for (size_t ctr = 0; ctr < num_of_values; ctr++) {
        // a little strange that this eval is processed with the input
        // tuples to the base expression.
        const voltdb::NValue right_value = this->m_values[ctr]->eval(tuple1, tuple2);
        if (left_value.op_equals(right_value).isTrue()) {
            return NValue::getTrue();
        }
    }
    return NValue::getFalse();
}

void
InComparisonExpression::substitute(const NValueArray &params)
{
    // allow children to substitute.
    this->m_left->substitute(params);

    // and update values stored here.
    for (size_t ctr = 0, cnt = m_values.size(); ctr < cnt; ctr++) {
        m_values[ctr]->substitute(params);
    }
}

std::string
InComparisonExpression::debugInfo(const std::string &spacer) const
{
    std::ostringstream buffer;
    buffer << spacer << "Values[" << this->m_values.size() << "]\n";
    for (size_t ctr = 0, cnt = this->m_values.size(); ctr < cnt; ctr++) {
        buffer << this->m_values[ctr]->debug(spacer);
    }
    return (buffer.str());
}

}

