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
#include "StreamPredicateHashRange.h"

namespace voltdb
{

/*
 * Produce a list of StreamPredicateHashRange objects by parsing predicate range strings.
 * Add error messages to errmsg.
 * Return true on success.
 */
bool StreamPredicateHashRange::parse(
        const std::vector<std::string> &predicate_strings,
        StreamPredicateList<StreamPredicateHashRange>& predicates_out,
        std::ostringstream& errmsg)
{
    bool success = true;
    for (std::vector<std::string>::const_iterator predi = predicate_strings.begin(),
                                                      e = predicate_strings.end();
         predi != e; ++predi) {
        std::vector<std::string> snums;
        try {
            boost::split(snums, *predi, boost::is_any_of("-"));
            if (snums.size() == 2) {
                size_t minHash = boost::lexical_cast<size_t>(snums[0]);
                size_t maxHash = boost::lexical_cast<size_t>(snums[1]);
                if (predicates_out.empty()) {
                    if (minHash != 0) {
                        errmsg << "First min hash, " << minHash << ", is non-zero"
                        << " for range predicate '" << *predi << "'" << std::endl;
                        success = false;
                    }
                }
                else {
                    size_t prevMaxHash = predicates_out.back().m_maxHash;
                    if (minHash != prevMaxHash + 1) {
                        errmsg << "Min hash " << minHash
                        << " is not previous max (" << prevMaxHash << ") + 1"
                        << " for range predicate '" << *predi << "'" << std::endl;
                        success = false;
                    }
                }
                if (maxHash <= minHash) {
                    errmsg << "Max <= min for range predicate '" << *predi << "'" << std::endl;
                    success = false;
                }
                if (!errmsg.str().empty()) {
                    predicates_out.push_back(new StreamPredicateHashRange(minHash, maxHash));
                    success = false;
                }
            }
            else {
                errmsg << "Bad range predicate '" << *predi << std::endl;
                success = false;
            }
        }
        catch(std::exception &exc) {
            errmsg << "Failed to parse range predicate '" << *predi
            << "': " << exc.what() << std::endl;
            success = false;
        }
    }
    return success;
}

/**
 * Generate a hash code using modulus.
 */
static int modulusHash(const NValue& value, int32_t totalPartitions)
{
    // Default to partition 0, e.g. when value is null.
    int hash = 0;
    if (!value.isNull())
    {
        ValueType val_type = ValuePeeker::peekValueType(value);
        switch (val_type)
        {
        case VALUE_TYPE_TINYINT:
        case VALUE_TYPE_SMALLINT:
        case VALUE_TYPE_INTEGER:
        case VALUE_TYPE_BIGINT:
        {
            hash = (int)(ValuePeeker::peekAsRawInt64(value) % totalPartitions);
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

bool StreamPredicateHashRange::accept(
        PersistentTable &table,
        const TableTuple &tuple,
        int32_t totalPartitions) const
{
    int partitionColumn = table.partitionColumn();
    if (partitionColumn == -1) {
        return true;
    }
    int hash = modulusHash(tuple.getNValue(partitionColumn), totalPartitions);
    return (hash >= m_minHash && hash <= m_maxHash);
}

} // namespace voltdb
