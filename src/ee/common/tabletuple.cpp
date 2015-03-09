/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#include <cstdlib>
#include <sstream>
#include <cassert>
#include "common/tabletuple.h"
#include "common/common.h"
#include "common/debuglog.h"
#include "common/FatalException.hpp"

namespace voltdb {

std::string TableTuple::debug(const std::string& tableName) const {
    assert(m_schema);
    assert(m_data);

    std::ostringstream buffer;
    if (tableName.empty()) {
        buffer << "TableTuple(no table) ->";
    } else {
        buffer << "TableTuple(" << tableName << ") ->";
    }

    if (isActive() == false) {
        buffer << " <DELETED>";
    } else {
        for (int ctr = 0; ctr < m_schema->columnCount(); ctr++) {
            buffer << "(";
            if (isNull(ctr)) {
                buffer << "<NULL>";
            } else {
                buffer << getNValue(ctr).debug();
            }
            buffer << ")";
        }
    }

    uint64_t addressNum = (uint64_t)address();
    buffer << " @" << addressNum;

    std::string ret(buffer.str());
    return ret;
}

std::string TableTuple::debugNoHeader() const {
    assert(m_schema);
    assert(m_data);

    std::ostringstream buffer;
    buffer << "TableTuple(notable) ->";

    for (int ctr = 0; ctr < m_schema->columnCount(); ctr++) {
        if (isNull(ctr)) {
            buffer << "<NULL>";
        } else {
            buffer << "(" << getNValue(ctr).debug() << ")";
        }
    }
    std::string ret(buffer.str());
    return ret;
}

}
