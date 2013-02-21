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

#ifndef STREAMPREDICATEHASHRANGE_H_
#define STREAMPREDICATEHASHRANGE_H_

#include "StreamPredicate.h"
#include "StreamPredicateList.h"

namespace voltdb
{

class TableTuple;
class PersistentTable;

/**
 * A simple testing-only predicate type for filtering output streams based
 * on modulus hashing and a range check. Provides predictable partition
 * assignments for test validation.
 * IMPORTANT: Not for product use.
 */
class StreamPredicateHashRange : public StreamPredicate
{
public:

    virtual ~StreamPredicateHashRange() {}

    /**
     * Required static method to parse predicates out of the strings provided.
     * Type-specific signature is resolved by the StreamPredicateList template.
     * Return true on success.
     */
    static bool parse(const std::vector<std::string> &predicate_strings,
                      StreamPredicateList<StreamPredicateHashRange>& predicates_out,
                      std::ostringstream& errmsg);

    /**
     * Accept or reject a tuple.
     * Return true if the predicate accepts the tuple.
     */
    virtual bool accept(PersistentTable &table, const TableTuple &tuple, int32_t totalPartitions) const;

private:

    // Should go through parse() factory method to construct a predicate.
    StreamPredicateHashRange(std::size_t minHash, std::size_t maxHash)
        : m_minHash(minHash), m_maxHash(maxHash) {}

    std::size_t m_minHash;
    std::size_t m_maxHash;
};

} // namespace voltdb

#endif /* defined(STREAMPREDICATEHASHRANGE_H_) */
