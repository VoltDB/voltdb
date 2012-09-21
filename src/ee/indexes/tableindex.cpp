/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

#include <iostream>
#include "indexes/tableindex.h"

using namespace voltdb;

TableIndex::TableIndex(const TupleSchema *keySchema, const TableIndexScheme &scheme) :
    m_scheme(scheme),
    m_keySchema(keySchema),

    // initialize all the counters to zero
    m_lookups(0),
    m_inserts(0),
    m_deletes(0),
    m_updates(0),

    m_stats(this)
{}

TableIndex::~TableIndex()
{
    voltdb::TupleSchema::freeTupleSchema(const_cast<TupleSchema*>(m_keySchema));
}

std::string TableIndex::debug() const
{
    std::ostringstream buffer;
    buffer << getTypeName() << "(" << getName() << ")";
    buffer << (isUniqueIndex() ? " UNIQUE " : " NON-UNIQUE ");
    //
    // Columns
    //
    const std::vector<int> &column_indices_vector = getColumnIndices();
    if (m_keySchema->columnCount() != column_indices_vector.size()) {
    buffer << " *** COLUMN COUNT DISPARITY -> " << column_indices_vector.size() << " VS ";
    buffer << m_keySchema->columnCount() << " *** ";

    }
    buffer << " -> " << column_indices_vector.size() << " Columns[";
    std::string add = "";
    for (int ctr = 0; ctr < column_indices_vector.size(); ctr++) {
        buffer << add << ctr << "th entry=" << column_indices_vector[ctr]
               << "th (" << voltdb::valueToString(m_keySchema->columnType(ctr))
               << ") column in parent table";
        add = ", ";
    }
    buffer << "] --- size: " << getSize();

    std::string ret(buffer.str());
    return (ret);
}

IndexStats* TableIndex::getIndexStats() {
    return &m_stats;
}

void TableIndex::printReport()
{
    std::cout << m_scheme.name << ",";
    std::cout << getTypeName() << ",";
    std::cout << m_lookups << ",";
    std::cout << m_inserts << ",";
    std::cout << m_deletes << ",";
    std::cout << m_updates << std::endl;
}

bool TableIndex::equals(const TableIndex *other) const
{
    //TODO Do something useful here!
    return true;
}
