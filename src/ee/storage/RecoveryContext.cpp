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
#include "storage/RecoveryContext.h"
#include "common/RecoveryProtoMessageBuilder.h"
#include "common/DefaultTupleSerializer.h"
#include "common/TupleOutputStream.h"
#include "common/TupleOutputStreamProcessor.h"
#include "storage/persistenttable.h"

#include <cstdio>
using namespace std;

namespace voltdb {
RecoveryContext::RecoveryContext(
        PersistentTable &table,
        PersistentTableSurgeon &surgeon,
        int32_t partitionId,
        TupleSerializer &serializer,
        int32_t tableId) :
    TableStreamerContext(table, surgeon, partitionId, serializer),
    m_firstMessage(true),
    m_iterator(getTable().iterator()),
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
        m_iterator = getTable().iterator();
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
    uint32_t allocatedTupleCount = static_cast<uint32_t>(getTable().allocatedTupleCount());
    RecoveryProtoMsgBuilder message(
            m_recoveryPhase,
            m_tableId,
            allocatedTupleCount,
            out,
            &m_serializer,
            getTable().schema());
    TableTuple tuple(getTable().schema());
    while (message.canAddMoreTuples() && m_iterator.next(tuple)) {
        message.addTuple(tuple);
    }
    message.finalize();
    return true;
}

/**
 * Mandatory TableStreamContext override.
 */
int64_t RecoveryContext::handleStreamMore(TupleOutputStreamProcessor &outputStreams,
                                          std::vector<int> &retPositions) {
    if (outputStreams.size() != 1) {
        throwFatalException("RecoveryContext::handleStreamMore: Expect 1 output stream "
                            "for recovery, received %ld", outputStreams.size());
    }
    /*
     * Table ids don't change during recovery because
     * catalog changes are not allowed.
     */
    bool hasMore = nextMessage(&outputStreams[0]);
    // Non-zero if some tuples remain, we're just not sure how many.
    int64_t remaining = (hasMore ? 1 : 0);
    for (size_t i = 0; i < outputStreams.size(); i++) {
        retPositions.push_back((int)outputStreams.at(i).position());
    }
    return remaining;
}

}
