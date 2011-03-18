/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "storage/RecoveryContext.h"
#include "common/RecoveryProtoMessageBuilder.h"
#include "common/DefaultTupleSerializer.h"
#include "storage/persistenttable.h"

#include <cstdio>
using namespace std;

namespace voltdb {
RecoveryContext::RecoveryContext(PersistentTable *table, int32_t tableId) :
        m_table(table),
        m_firstMessage(true),
        m_iterator(m_table->iterator()),
        m_tableId(tableId),
        m_recoveryPhase(RECOVERY_MSG_TYPE_SCAN_TUPLES) {
}

/*
 * Generate the next recovery message. Eventually returns a message containing the message type
 * RECOVERY_MSG_TYPE_COMPLETE indicating that all tuple data and updates to shipped data
 * have been sent.
 */
bool RecoveryContext::nextMessage(ReferenceSerializeOutput *out) {
    if (m_recoveryPhase == RECOVERY_MSG_TYPE_COMPLETE) {
        return false;
    }

    // We need to initialize the iterator over the table the first
    // time we call here.  The implementation in Java guarantees that
    // once we start generating these recovery messages that no
    // additional transactions will change the table state (and leave
    // us with an inconsistent iterator).
    if (m_firstMessage)
    {
        m_iterator = m_table->iterator();
        m_firstMessage = false;

    }

    if (!m_iterator.hasNext()) {
        m_recoveryPhase = RECOVERY_MSG_TYPE_COMPLETE;
        out->writeByte(static_cast<int8_t>(RECOVERY_MSG_TYPE_COMPLETE));
        out->writeInt(m_tableId);
//        out->writeTextString(m_table->m_Tuple);
        // last message gets the export stream counter
//        long seqNo = 0; size_t offset = 0;
//        m_table->getExportStreamSequenceNo(seqNo, offset);
//        out->writeLong(seqNo);
//        out->writeLong((long) offset);
        //No tuple count added to message because completion message is only used in Java
        return false;
    }
    DefaultTupleSerializer serializer;
    //Use allocated tuple count to size stuff at the other end
    uint32_t allocatedTupleCount = static_cast<uint32_t>(m_table->allocatedTupleCount());
    RecoveryProtoMsgBuilder message(
            m_recoveryPhase,
            m_tableId,
            allocatedTupleCount,
            out,
            &m_serializer,
            m_table->schema());
    TableTuple tuple(m_table->schema());
    while (message.canAddMoreTuples() && m_iterator.next(tuple)) {
        message.addTuple(tuple);
    }
    message.finalize();
    return true;
}
}
