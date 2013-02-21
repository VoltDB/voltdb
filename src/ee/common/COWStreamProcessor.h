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

#ifndef COWSTREAMPROCESSOR_H_
#define COWSTREAMPROCESSOR_H_

#include <cstddef>
#include <boost/ptr_container/ptr_vector.hpp>
#include "StreamPredicateList.h"
#include "StreamPredicateHashRange.h"

namespace voltdb {

class TupleSerializer;
class TableTuple;
class PersistentTable;
class TupleSerializer;
class COWStream;

typedef StreamPredicateList<StreamPredicateHashRange> COWPredicateList;

/** COWStream processor. Manages and outputs to multiple COWStream's. */
class COWStreamProcessor : public boost::ptr_vector<COWStream> {

public:

    /** Default constructor. */
    COWStreamProcessor();

    /** Constructor with initial size. */
    COWStreamProcessor(std::size_t nBuffers);

    /** Constructor for a single stream. Convenient for backward compatibility in tests. */
    COWStreamProcessor(void *data, std::size_t length);

    /** Convenience method to create and add a new COWStream. */
    COWStream &add(void *data, std::size_t length);

    /** Start serializing. */
    void open(PersistentTable &table,
              std::size_t maxTupleLength,
              int32_t partitionId,
              int32_t totalPartitions);

    /** Stop serializing. */
    void close();

    /**
     * Write a tuple to the output streams.
     * Expects buffer space was already checked.
     * Maintains the total byte counter provided by the caller.
     * Returns true when one of the output buffers fills.
     */
    bool writeRow(TupleSerializer &serializer, TableTuple &tuple, std::size_t &totalBytesSerialized);

private:

    /** The maximum tuple length. */
    std::size_t m_maxTupleLength;

    /** Predicates for filtering. */
    COWPredicateList *m_predicates;

    /** Table receiving tuples. */
    PersistentTable *m_table;

    /** Total number of partitions (for hashing). */
    int32_t m_totalPartitions;

    /** Private method used by constructors, etc. to clear state. */
    void clearState();
};

} // namespace voltdb

#endif // COWSTREAMPROCESSOR_H_
