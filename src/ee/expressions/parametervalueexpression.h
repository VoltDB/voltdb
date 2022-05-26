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

#ifndef HSTOREPARAMETERVALUEEXPRESSION_H
#define HSTOREPARAMETERVALUEEXPRESSION_H

#include "common/NValue.hpp"

#include "expressions/abstractexpression.h"

#include <vector>
#include <string>
#include <sstream>
#include <common/debuglog.h>

namespace voltdb {

class ParameterValueExpression : public AbstractExpression {
public:

    // Constructor to initialize the PVE from the static parameter vector
    // from the VoltDBEngine instance. After the construction the PVE points
    // to the NValue from the global vector.
    ParameterValueExpression(int value_idx);

    // Constructor to use for testing purposes
    ParameterValueExpression(int value_idx, voltdb::NValue* paramValue) :
        m_valueIdx(value_idx), m_paramValue(paramValue) {
    }

    voltdb::NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        vassert(m_paramValue != NULL);
        return *m_paramValue;
    }

    bool hasParameter() const {
        // this class represents a parameter.
        return true;
    }

    std::string debugInfo(const std::string &spacer) const {
        std::ostringstream buffer;
        buffer << spacer << "OptimizedParameter[" << this->m_valueIdx << "]\n";
        return (buffer.str());
    }

    int getParameterId() const {
        return this->m_valueIdx;
    }

  private:
    int m_valueIdx;

    voltdb::NValue *m_paramValue;
};

}
#endif
