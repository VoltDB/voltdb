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

#ifndef STREAMPREDICATE_H_
#define STREAMPREDICATE_H_

namespace voltdb
{
class TableTuple;
class PersistentTable;
class StreamPredicateList;

/** A predicate for filtering output streams. */
class StreamPredicate
{
    friend class StreamPredicateList;

public:

    virtual ~StreamPredicate() {}

    /**
     * Accept or reject a tuple.
     * Return true if the predicate accepts the tuple.
     */
    bool accept(PersistentTable &table, const TableTuple &tuple, int32_t totalPartitions) const;

private:

    // Should go through parse() factory method to construct a predicate.
    //TODO: min/max hash is temporary pending full expression support.
    StreamPredicate(int32_t minHash, int32_t maxHash)
      : m_minHash(minHash), m_maxHash(maxHash) {}

    int32_t m_minHash;
    int32_t m_maxHash;
};

}

#endif /* defined(STREAMPREDICATE_H_) */
