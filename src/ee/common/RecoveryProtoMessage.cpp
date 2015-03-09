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

#include "common/RecoveryProtoMessage.h"
#include "common/FatalException.hpp"
#include "common/types.h"
#include "common/Pool.hpp"

namespace voltdb {

/*
 * Prepare a recovery message for reading.
 */
RecoveryProtoMsg::RecoveryProtoMsg(ReferenceSerializeInputBE *in) :
        m_in(in),  m_type(static_cast<RecoveryMsgType>(in->readByte())),
        m_tableId(in->readInt()) {
    assert(m_in);
    assert(m_type != RECOVERY_MSG_TYPE_SCAN_COMPLETE);
    int32_t totalTupleCount = in->readInt();
    m_totalTupleCount = *reinterpret_cast<uint32_t*>(&totalTupleCount);
    if (m_type == RECOVERY_MSG_TYPE_COMPLETE)
        m_exportStreamSeqNo = in->readLong();
}

/*
 * Retrieve the type of this recovery message.
 */
RecoveryMsgType RecoveryProtoMsg::msgType() {
    return m_type;
}

/*
 * Retrieve the type of this recovery message.
 */
CatalogId RecoveryProtoMsg::tableId() {
    return m_tableId;
}

uint32_t RecoveryProtoMsg::totalTupleCount() {
    return m_totalTupleCount;
}

ReferenceSerializeInputBE* RecoveryProtoMsg::stream() {
    return m_in;
}
}
