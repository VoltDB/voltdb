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

#ifndef COWSTREAM_H_
#define COWSTREAM_H_

#include "serializeio.h"

namespace voltdb {

class TupleSerializer;
class TableTuple;

/**
 * Serialization output class with some additional data that allows the
 * filtered COW processing to manage the stream.
 */
class COWStream : public ReferenceSerializeOutput {

public:

    /**
     * Constructor.
     */
    COWStream(void *data, size_t length);

    /**
     * Destructor.
     */
    virtual ~COWStream();

    /**
     * Write the header and save space for the row count.
     */
    size_t startRows(int32_t partitionId);

    /**
     * Write a tuple and return the number of bytes written.
     */
    size_t writeRow(TupleSerializer &serializer, const TableTuple &tuple);

    /**
     * Return true if nbytes can fit in the buffer's remaining space.
     */
    bool canFit(size_t nbytes) const;

    /**
     * Write the row count when finished with an output sequence.
     */
    void endRows();

private:

    int32_t m_rowCount;
    size_t  m_rowCountPosition;
};

/** Convenience type alias for vector of COWStream */
typedef boost::ptr_vector<COWStream> COWStreamList;

} // namespace voltdb

#endif // COWSTREAM_H_
