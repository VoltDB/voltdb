/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

#pragma once

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
    using super = boost::ptr_vector<TupleOutputStream>;

    /** Pause serialization after this many bytes per partition. */
    static constexpr std::size_t BYTES_SERIALIZED_THRESHOLD = 512 * 1024;

    /** The maximum tuple length. */
    std::size_t m_maxTupleLength = 0;

    /** Table receiving tuples. */
    bool m_opened = false;

    /** Predicates for filtering. May remain non-NULL after open() if empty. */
    StreamPredicateList const* m_predicates = nullptr;

    /** Vector of booleans that indicates whether the predicate return true means the row should be deleted */
    std::vector<bool> const* m_predicateDeletes = nullptr;
public:
    TupleOutputStreamProcessor() = default;

    /** Constructor with initial size. */
    TupleOutputStreamProcessor(std::size_t nBuffers);

    /** Constructor for a single stream. Convenient for backward compatibility in tests. */
    TupleOutputStreamProcessor(void *data, std::size_t length);

    /** Convenience method to create and add a new TupleOutputStream. */
    TupleOutputStream &add(void *data, std::size_t length);

    /** Start serializing. */
    void open(std::size_t maxTupleLength,
              int32_t partitionId,
              StreamPredicateList const& predicates,
              std::vector<bool> const& predicateDeletes);

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

};

} // namespace voltdb

