/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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


#include "vectorcomparisonexpression.hpp"

namespace voltdb {

bool TupleExtractor::isNullValue(const ValueType& value) const
{
    if (!value.isNullTuple()){
        int schemaSize = m_tuple.getSchema()->columnCount();
        for (int columnIdx = 0; columnIdx < schemaSize; ++columnIdx)
        {
            if (m_tuple.isNull(columnIdx)) {
                return true;
            }
        }
        return false;
    }
    return true;
}

template <>
bool compare_tuple<CmpEq>(const TableTuple& tuple1, const TableTuple& tuple2)
{
    assert(tuple1.getSchema()->columnCount() == tuple2.getSchema()->columnCount());
    int schemaSize = tuple1.getSchema()->columnCount();
    CmpEq comp;
    for (int columnIdx = 0; columnIdx < schemaSize; ++columnIdx)
    {
        if (comp.cmp(tuple1.getNValue(columnIdx), tuple2.getNValue(columnIdx)).isFalse())
        {
            return false;
        }
    }
    return true;
}

template <>
bool compare_tuple<CmpNe>(const TableTuple& tuple1, const TableTuple& tuple2)
{
    // a != b <=> !(a == B)
    return !compare_tuple<CmpEq>(tuple1, tuple2);
}

template <>
bool compare_tuple<CmpGte>(const TableTuple& tuple1, const TableTuple& tuple2)
{
    // a >= b <=> !(a < b)
    return !compare_tuple<CmpLt>(tuple1, tuple2);
}

template <>
bool compare_tuple<CmpLte>(const TableTuple& tuple1, const TableTuple& tuple2)
{
    // a <= b <=> !(a > b)
    return !compare_tuple<CmpGt>(tuple1, tuple2);
}

}
