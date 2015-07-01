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

#ifndef TUPLEOUTPUTSTREAM_H_
#define TUPLEOUTPUTSTREAM_H_

#include <cstddef>
#include <boost/ptr_container/ptr_vector.hpp>
#include "serializeio.h"

namespace voltdb {

class TupleSerializer;
class TableTuple;
class PersistentTable;
class TupleSerializer;

/**
 * Serialization output class with some additional data that allows the
 * filtered COW processing to manage the stream.
 */
class TupleOutputStream : public ReferenceSerializeOutput {

public:

    /**
     * Constructor.
     */
    TupleOutputStream(void *data, std::size_t length);

    /**
     * Destructor.
     */
    virtual ~TupleOutputStream();

    /**
     * Write the header and save space for the row count.
     */
    std::size_t startRows(int32_t partitionId);

    /**
     * Write a tuple and return the number of bytes written.
     */
    std::size_t writeRow(TupleSerializer &tupleSerializer,
                         const TableTuple &tuple);

    /**
     * Return true if nbytes can fit in the buffer's remaining space.
     */
    bool canFit(size_t nbytes) const;

    /**
     * Write the row count when finished with an output sequence.
     */
    void endRows();

    /**
     * Access the total bytes serialized counter.
     */
    std::size_t getTotalBytesSerialized() const {
        return m_totalBytesSerialized;
    }

    /**
     * Access the serialized row counter
     */
    int32_t getSerializedRowCount() const {
        return m_rowCount;
    }

private:

    int32_t     m_rowCount;
    std::size_t m_rowCountPosition;
    /** Keep track of bytes written for throttling to yield control. */
    std::size_t m_totalBytesSerialized;
};

} // namespace voltdb

#endif // TUPLEOUTPUTSTREAM_H_
