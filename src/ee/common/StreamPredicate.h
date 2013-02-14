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

#include <string>
#include <boost/ptr_container/ptr_vector.hpp>

namespace voltdb {

class TableTuple;
class PersistentTable;
class StreamPredicate;

typedef boost::ptr_vector<StreamPredicate> StreamPredicateList;

class StreamPredicate {
public:
    virtual ~StreamPredicate() {}

    // Factory method to parse a bunch of predicate strings and return a vector
    // of StreamPredicate objects. Sanity checks the sequence of predicates for
    // completeness, etc..
    static void parse(const std::vector<std::string> &predicate_strings,
                      StreamPredicateList &predicates_out);

    // Return true if the predicate accepts the tuple.
    bool accept(PersistentTable &table,
                const TableTuple &tuple,
                int32_t totalPartitions) const;

private:
    // Should go through parse() factory method.
    StreamPredicate(size_t minHash, size_t maxHash) : m_minHash(minHash), m_maxHash(maxHash) {}

    //TODO: Abstract out what's stored here. Someday it won't be only ranges.
    int m_minHash;
    int m_maxHash;
};

}

#endif /* defined(STREAMPREDICATE_H_) */
