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

#include "common/RecoveryProtoMessageBuilder.h"
#include "common/FatalException.hpp"
#include "common/types.h"
#include "common/Pool.hpp"
#include "common/TupleSerializer.h"

namespace voltdb {
/*
 * Construct a recovery message to populate with recovery data
 */
RecoveryProtoMsgBuilder::RecoveryProtoMsgBuilder(
        const RecoveryMsgType type,
        CatalogId tableId,
        uint32_t totalTupleCount,
        ReferenceSerializeOutput *out,
        TupleSerializer *serializer,
        const TupleSchema *schema) :
    m_out(out),
    m_tupleCount(0),
    m_maxSerializedSize(serializer->getMaxSerializedTupleSize(schema))
{
    assert(m_out);
    m_out->writeByte(static_cast<int8_t>(type));
    m_out->writeInt(tableId);
    m_out->writeInt(*reinterpret_cast<int32_t*>(&totalTupleCount));
    m_tupleCountPosition = m_out->reserveBytes(sizeof(int32_t));
}

/*
 * Add a tuple to be inserted or updated at the recovering partition.
 */
void RecoveryProtoMsgBuilder::addTuple(TableTuple tuple) {
    assert(m_out);
    assert(canAddMoreTuples());
    tuple.serializeTo(*m_out);
    m_tupleCount++;
}

/*
 * Write the tuple count and any other information
 */
void RecoveryProtoMsgBuilder::finalize() {
    m_out->writeIntAt(m_tupleCountPosition, m_tupleCount);
}

bool RecoveryProtoMsgBuilder::canAddMoreTuples() {
    if (m_out->remaining() >= m_maxSerializedSize) {
        return true;
    }
    return false;
}
}
