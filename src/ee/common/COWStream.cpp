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

#include "COWStream.h"
#include "StreamPredicate.h"
#include "TupleSerializer.h"
#include "tabletuple.h"
#include <limits>

namespace voltdb {

COWStream::COWStream(void *data, std::size_t length)
  : ReferenceSerializeOutput(data, length), m_rowCount(0), m_rowCountPosition(0)
{
}

COWStream::~COWStream()
{
}

std::size_t COWStream::startRows(int32_t partitionId)
{
    writeInt(partitionId);
    m_rowCount = 0;
    m_rowCountPosition = reserveBytes(4);
    return m_rowCountPosition;
}

std::size_t COWStream::writeRow(TupleSerializer &serializer, const TableTuple &tuple)
{
    const std::size_t startPos = position();
    serializer.serializeTo(tuple, this);
    const std::size_t endPos = position();
    m_rowCount++;
    return endPos - startPos;
}

bool COWStream::canFit(std::size_t nbytes) const
{
    return (remaining() >= nbytes + sizeof(int32_t));
}

void COWStream::endRows()
{
    writeIntAt(m_rowCountPosition, m_rowCount);
}

} // namespace voltdb
