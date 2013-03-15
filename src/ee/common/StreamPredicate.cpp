/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

#include <string>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include "tabletuple.h"
#include "storage/persistenttable.h"
#include "StreamPredicate.h"

namespace voltdb
{

/**
 * Generate a hash code using modulus.
 */
static int32_t modulusHash(const NValue& nvalue, int32_t totalPartitions)
{
    // Default to partition 0, e.g. when value is null.
    int hash = 0;
    if (!nvalue.isNull())
    {
        ValueType val_type = ValuePeeker::peekValueType(nvalue);
        switch (val_type)
        {
        case VALUE_TYPE_TINYINT:
        case VALUE_TYPE_SMALLINT:
        case VALUE_TYPE_INTEGER:
        case VALUE_TYPE_BIGINT:
        {
            int64_t ivalue = ValuePeeker::peekAsRawInt64(nvalue);
            hash = (int32_t)(ivalue % totalPartitions);
            break;
        }
        // varbinary and varchar are unsupported because they aren't currently needed for testing.
        case VALUE_TYPE_VARBINARY:
        case VALUE_TYPE_VARCHAR:
        default:
            throwDynamicSQLException("Attempted to calculate the modulus hash of an unsupported type: %s",
                                     getTypeName(val_type).c_str());
        }
    }
    return hash;
}

//TODO: min/max hash is temporary pending full expression support.
bool StreamPredicate::accept(
        PersistentTable &table,
        const TableTuple &tuple,
        int32_t totalPartitions) const
{
    int partitionColumn = table.partitionColumn();
    if (partitionColumn == -1) {
        return true;
    }
    int32_t hash = modulusHash(tuple.getNValue(partitionColumn), totalPartitions);
    return (hash >= m_minHash && hash <= m_maxHash);
}

} // namespace voltdb
