/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#include "common/UndoReleaseAction.h"

namespace voltdb {

class ExecuteTaskUndoGenerateDREventAction : public voltdb::ReleaseOnlyAction {
public:
    ExecuteTaskUndoGenerateDREventAction(
            AbstractDRTupleStream* drStream, AbstractDRTupleStream* drReplicatedStream,
            CatalogId partitionId,
            DREventType type,
            int64_t spHandle, int64_t uniqueId, ByteArray payloads)
        : m_drStream(drStream)
        , m_drReplicatedStream(drReplicatedStream)
        , m_partitionId(partitionId)
        , m_type(type)
        , m_spHandle(spHandle)
        , m_uniqueId(uniqueId)
        , m_payloads(payloads)
    { }

    void release() {
        // TODO: skip generate DR_ELASTIC_REBALANCE event on replicated stream, remove this once the DR ReplicatedTable Stream has been removed
        // Here XOR is used for the type check and the stream started check. On one hand, stream should not
        // generate any event other than DR_STREAM_START if it is not started yet, on the other hand,
        // stream should not generate DR_STREAM_START if it is already started. The same logic is applied to
        // the partition stream in the last IF block.
        if (m_drReplicatedStream && (m_type != DR_ELASTIC_REBALANCE) &&
            ((m_type == DR_STREAM_START) != m_drReplicatedStream->drStreamStarted())) {
            m_drReplicatedStream->generateDREvent(m_type, m_spHandle, m_uniqueId, m_payloads);
        }

        if (m_type == DR_ELASTIC_CHANGE) {
            ReferenceSerializeInputBE input(m_payloads.data(), 8);
            int oldPartitionCnt = input.readInt();
            if (m_partitionId >= oldPartitionCnt && m_partitionId != 16383) {
                // skip the drStreamStarted() check as this DR_ELASTIC_CHANGE will be transformed into DR_STREAM_START
                m_drStream->generateDREvent(m_type, m_spHandle, m_uniqueId, m_payloads);
                return;
            }
        }

        if ((m_type == DR_STREAM_START) != m_drStream->drStreamStarted()) {
            m_drStream->generateDREvent(m_type, m_spHandle, m_uniqueId, m_payloads);
        }
    }

private:
    AbstractDRTupleStream* m_drStream;
    AbstractDRTupleStream* m_drReplicatedStream;
    CatalogId m_partitionId;
    DREventType m_type;
    int64_t m_spHandle;
    int64_t m_uniqueId;
    ByteArray m_payloads;
};

}

#endif
