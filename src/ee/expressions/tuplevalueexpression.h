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

#ifndef HSTORETUPLEVALUEEXPRESSION_H
#define HSTORETUPLEVALUEEXPRESSION_H

#include "expressions/abstractexpression.h"
#include "common/tabletuple.h"

#include <string>
#include <sstream>

namespace voltdb {

class SerializeInput;
class SerializeOutput;

class TupleValueExpression : public AbstractExpression {
  public:
    TupleValueExpression(int value_idx, std::string tableName, int table_idx)
        : AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE)
    {
        VOLT_TRACE("OptimizedTupleValueExpression %d using tupleIdx %d valueIdx", m_type, table_idx, value_idx);
        this->tuple_idx = table_idx;
        this->value_idx = value_idx;
        this->table_name = tableName;
    };

    virtual voltdb::NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        if (tuple_idx == 0) {
            assert(tuple1);
            if ( ! tuple1 ) {
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_SQL,
                                              "TupleValueExpression::"
                                              "eval:"
                                              " Couldn't find tuple 1 (possible index scan planning error)");
            }
            return tuple1->getNValue(this->value_idx);
        }
        else {
            assert(tuple2);
            if ( ! tuple2 ) {
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_SQL,
                                              "TupleValueExpression::"
                                              "eval:"
                                              " Couldn't find tuple 2 (possible index scan planning error)");
            }
            return tuple2->getNValue(this->value_idx);
        }
    }

    std::string debugInfo(const std::string &spacer) const {
        std::ostringstream buffer;
        buffer << spacer << "Optimized Column Reference[" << this->tuple_idx << ", " << this->value_idx << "]\n";
        return (buffer.str());
    }

    int getColumnId() const {return this->value_idx;}

    std::string getTableName() {
        return table_name;
    }

  protected:

    int tuple_idx;           // which tuple. defaults to tuple1
    int value_idx;           // which (offset) column of the tuple
    std::string table_name;
};

}
#endif
