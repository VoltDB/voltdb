/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
    if (value.isNullTuple()) {
        return true;
    }
    int schemaSize = m_tuple.getSchema()->columnCount();
    for (int columnIdx = 0; columnIdx < schemaSize; ++columnIdx) {
        if (m_tuple.isNull(columnIdx)) {
            return true;
        }
    }
    return false;
}

}
