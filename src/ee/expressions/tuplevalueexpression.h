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

#pragma once

#include "expressions/abstractexpression.h"
#include "common/tabletuple.h"

#include <string>
#include <sstream>

namespace voltdb {

class TupleValueExpression : public AbstractExpression {
  public:
    TupleValueExpression(const int tableIdx, const int valueIdx)
        : AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE), tuple_idx(tableIdx), value_idx(valueIdx) {
        VOLT_TRACE("OptimizedTupleValueExpression %d using tupleIdx %d valueIdx %d", m_type, tableIdx, valueIdx);
    };

    virtual voltdb::NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        if (tuple_idx == 0) {
            vassert(tuple1);
            if (! tuple1 ) {
                throw SerializableEEException(
                        "TupleValueExpression::eval: Couldn't find tuple 1 (possible index scan planning error)");
            }
            return tuple1->getNValue(value_idx);
        } else {
            vassert(tuple2);
            if (! tuple2 ) {
                throw SerializableEEException(
                        "TupleValueExpression::eval: Couldn't find tuple 2 (possible index scan planning error)");
            }
            return tuple2->getNValue(value_idx);
        }
    }

    std::string debugInfo(const std::string &spacer) const {
        std::ostringstream buffer;
        buffer << spacer << "Optimized Column Reference[" << tuple_idx << ", " << value_idx << "]\n";
        return (buffer.str());
    }

    int getColumnId() const {return this->value_idx;}

  protected:

    const int tuple_idx;           // which tuple
    const int value_idx;           // which (offset) column of the tuple
};

}
