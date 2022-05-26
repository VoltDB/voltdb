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

#include <cstdlib>
#include <sstream>
#include "common/tabletuple.h"

namespace voltdb {

std::string TableTuple::debug(const std::string& tableName, bool skipNonInline) const {
    vassert(m_schema);
    vassert(m_data);

    std::ostringstream buffer;
    if (tableName.empty()) {
       buffer << "TableTuple(no table) ->";
    } else {
       buffer << "TableTuple(" << tableName << ") ->";
    }

    if (isActive() == false) {
       buffer << " <DELETED> ";
    }
    for (int ctr = 0; ctr < m_schema->columnCount(); ctr++) {
        buffer << "(";
        const TupleSchema::ColumnInfo *colInfo = m_schema->getColumnInfo(ctr);
        if (isVariableLengthType(colInfo->getVoltType()) && !colInfo->inlined && skipNonInline) {
            StringRef* sr = *reinterpret_cast<StringRef**>(getWritableDataPtr(colInfo));
            buffer << "<non-inlined value @" << static_cast<void*>(sr) << ">";
        } else {
            try {
                buffer << getNValue(ctr).debug();
            } catch (SQLException const& e) {      // hack: help get away with corrupted data in exception path
                char b[128];
                strncpy(b, e.what(), 128);
                b[sizeof b - 1] = '\0';
                buffer << "{?? [" << ctr << "] Got SQLException: "
                    << b << (strlen(e.what()) > sizeof b ? "..." : "")
                    << " ??}";
                buffer << " @" << static_cast<void const*>(address());
                return buffer.str();
            }
        }
        buffer << ")";
    }

    if (m_schema->hiddenColumnCount() > 0) {
        buffer << " hidden->";

        for (int ctr = 0; ctr < m_schema->hiddenColumnCount(); ctr++) {
            buffer << "(";
            vassert(! isVariableLengthType(
                        m_schema->getHiddenColumnInfo(ctr)->getVoltType()));
            buffer << getHiddenNValue(ctr).debug() << ")";
        }
    }

    buffer << " @" << static_cast<const void*>(address());

    return buffer.str();
}

std::string TableTuple::debugNoHeader() const {
    vassert(m_schema);
    vassert(m_data);
    return debug("");
}

}
