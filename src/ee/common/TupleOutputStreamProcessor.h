/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#ifndef TUPLEOUTPUTSTREAMPROCESSOR_H_
#define TUPLEOUTPUTSTREAMPROCESSOR_H_

#include <cstddef>
#include <boost/ptr_container/ptr_vector.hpp>
#include "StreamPredicateList.h"
#include "common/HiddenColumnFilter.h"

namespace voltdb {
class TableTuple;
class PersistentTable;
class TupleOutputStream;
class StreamPredicateList;

/** TupleOutputStream processor. Manages and outputs to multiple TupleOutputStream's. */
class TupleOutputStreamProcessor : public boost::ptr_vector<TupleOutputStream> {

public:

    /** Default constructor. */
    TupleOutputStreamProcessor();

    /** Constructor with initial size. */
    TupleOutputStreamProcessor(std::size_t nBuffers);

    /** Constructor for a single stream. Convenient for backward compatibility in tests. */
    TupleOutputStreamProcessor(void *data, std::size_t length);

    /** Convenience method to create and add a new TupleOutputStream. */
    TupleOutputStream &add(void *data, std::size_t length);

    /** Start serializing. */
    void open(PersistentTable &table,
              std::size_t maxTupleLength,
              int32_t partitionId,
              StreamPredicateList &predicates,
              std::vector<bool> &predicateDeletes);

    /** Stop serializing. */
    void close();

    /**
     * Write a tuple to the output streams.
     * Expects buffer space was already checked.
     * numCopiesMade helps deletion logic decide when something is being moved.
     * Returns true when the caller should yield to allow other work to proceed.
     */
    bool writeRow(TableTuple &tuple,
                  const HiddenColumnFilter &hiddenColumnFilter,
                  bool *deleteRow = NULL);

private:

    /** The maximum tuple length. */
    std::size_t m_maxTupleLength;

    /** Pause serialization after this many bytes per partition. */
    static const std::size_t m_bytesSerializedThreshold = 512 * 1024;

    /** Table receiving tuples. */
    PersistentTable *m_table;

    /** Predicates for filtering. May remain non-NULL after open() if empty. */
    StreamPredicateList *m_predicates;

    /** Vector of booleans that indicates whether the predicate return true means the row should be deleted */
    std::vector<bool> *m_predicateDeletes;

    /** Private method used by constructors, etc. to clear state. */
    void clearState();
};

} // namespace voltdb

#endif // TUPLEOUTPUTSTREAMPROCESSOR_H_
