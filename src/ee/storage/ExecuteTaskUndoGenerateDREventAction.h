/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

#ifndef EXECUTETASK_H
#define EXECUTETASK_H

#include "common/UndoAction.h"

namespace voltdb {

class ExecuteTaskUndoGenerateDREventAction : public voltdb::UndoAction {
public:
        ExecuteTaskUndoGenerateDREventAction(AbstractDRTupleStream* drStream, AbstractDRTupleStream* drReplicatedStream , DREventType type, int64_t lastCommittedSpHandle,
                int64_t spHandle, int64_t uniqueId, ByteArray payloads)
    : m_drStream(drStream), m_drReplicatedStream(drReplicatedStream), m_type(type), m_lastCommittedSpHandle(lastCommittedSpHandle),m_spHandle(spHandle), m_uniqueId(uniqueId), m_payloads(payloads)
    {
    }

    void undo() {
    }

    void release() {
        m_drStream ->generateDREvent(m_type, m_lastCommittedSpHandle, m_spHandle, m_uniqueId, m_payloads);

        if (m_drReplicatedStream) {
            m_drReplicatedStream ->generateDREvent(m_type, m_lastCommittedSpHandle, m_spHandle, m_uniqueId, m_payloads);
        }
    }

private:
    AbstractDRTupleStream* m_drStream;
    AbstractDRTupleStream* m_drReplicatedStream;
    DREventType m_type;
    int64_t m_lastCommittedSpHandle;
    int64_t m_spHandle;
    int64_t m_uniqueId;
    ByteArray m_payloads;
};

}

#endif
