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
        ExecuteTaskUndoGenerateDREventAction(ExecutorContext* executorContext, DREventType type, int64_t lastCommittedSpHandle,
                int64_t spHandle, int64_t uniqueId, ByteArray payloads)
    : m_executorContext(executorContext), m_type(type), m_lastCommittedSpHandle(lastCommittedSpHandle),m_spHandle(spHandle), m_uniqueId(uniqueId), m_payloads(payloads)
    {
    }

    void undo() {
    }

    void release() {
        if (m_type == DR_STREAM_START || m_executorContext->drStream()->drStreamStarted()) {
            m_executorContext->drStream()->generateDREvent(m_type, m_lastCommittedSpHandle,
                                                           m_spHandle, m_uniqueId, m_payloads);
        }
        if (m_executorContext->drReplicatedStream() &&
            (m_type == DR_STREAM_START || m_executorContext->drReplicatedStream()->drStreamStarted())) {
            m_executorContext->drReplicatedStream()->generateDREvent(m_type, m_lastCommittedSpHandle,
                                                                     m_spHandle, m_uniqueId, m_payloads);
        }
    }

private:
    ExecutorContext* m_executorContext;
    DREventType m_type;
    int64_t m_lastCommittedSpHandle;
    int64_t m_spHandle;
    int64_t m_uniqueId;
    ByteArray m_payloads;
};

}

#endif
