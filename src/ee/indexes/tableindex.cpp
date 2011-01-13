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

#include <iostream>
#include "indexes/tableindex.h"

using namespace voltdb;

TableIndex::TableIndex(const TableIndexScheme &scheme) :
    m_scheme(scheme), name_(scheme.name), m_stats(this)
{
    column_indices_vector_ = scheme.columnIndices;
    column_types_vector_ = scheme.columnTypes;
    colCount_ = (int)column_indices_vector_.size();
    is_unique_index_ = scheme.unique;
    m_tupleSchema = scheme.tupleSchema;
    assert(column_types_vector_.size() == column_indices_vector_.size());
    column_indices_ = new int[colCount_];
    column_types_ = new ValueType[colCount_];
    for (int i = 0; i < colCount_; ++i)
    {
        column_indices_[i] = column_indices_vector_[i];
        column_types_[i] = column_types_vector_[i];
    }
    m_keySchema = scheme.keySchema;
    // initialize all the counters to zero
    m_lookups = m_inserts = m_deletes = m_updates = 0;
}
TableIndex::~TableIndex()
{
    delete[] column_indices_;
    delete[] column_types_;
    voltdb::TupleSchema::freeTupleSchema(m_keySchema);
}

std::string TableIndex::debug() const
{
    std::ostringstream buffer;
    buffer << this->getTypeName() << "(" << this->getName() << ")";
    buffer << (isUniqueIndex() ? " UNIQUE " : " NON-UNIQUE ");
    //
    // Columns
    //
    buffer << " -> Columns[";
    std::string add = "";
    for (int ctr = 0; ctr < this->colCount_; ctr++) {
        buffer << add << ctr << "th entry=" << this->column_indices_[ctr]
               << "th (" << voltdb::valueToString(column_types_[ctr])
               << ") column in parent table";
        add = ", ";
    }
    buffer << "] --- size: " << this->getSize();

    std::string ret(buffer.str());
    return (ret);
}

IndexStats* TableIndex::getIndexStats() {
    return &m_stats;
}

void TableIndex::printReport()
{
    std::cout << name_ << ",";
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
