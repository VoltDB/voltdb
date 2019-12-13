/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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

#include "kipling/messages/OffsetCommit.h"

namespace voltdb { namespace kipling {

OffsetCommitRequestPartition::OffsetCommitRequestPartition(const int16_t version, SerializeInputBE& request) :
        m_partitionIndex(readInt(request)), m_offset(readLong(request)) {
    if (version >= 6) {
        m_leaderEpoch = readInt(request);
    }
    if (version == 1) {
        m_timestamp = readLong(request);
    }
    m_metadata = readString(request);
}

OffsetCommitRequestTopic::OffsetCommitRequestTopic(const int16_t version, SerializeInputBE& request) :
        m_topic(readString(request)) {
    readRequestComponents(version, request, m_partitions);
}

OffsetCommitRequest::OffsetCommitRequest(const int16_t version, const NValue& groupId, SerializeInputBE& request) :
        GroupRequest(version, groupId) {
    if (version >= 1) {
        m_generationId = readInt(request);
        m_memberId = readString(request);
    }
    if (version >= 7) {
        m_groupInstanceId = readString(request);
    }
    if (version >= 2 && version <= 4) {
        // Read and discard legacy retention time
        readLong(request);
    }
    readRequestComponents(version, request, m_topics);
}

void OffsetCommitResponsePartition::write(const int16_t version, SerializeOutput& out) const {
    out.writeInt(m_partitionIndex);
    writeError(m_error, out);
}

void OffsetCommitResponseTopic::write(const int16_t version, SerializeOutput& out) const {
    writeString(m_topic, out);
    writeResponses(m_partitions, version, out);
}

void OffsetCommitResponse::write(const int16_t version, SerializeOutput& out) const {
    if (version >= 3) {
        out.writeInt(m_throttleTimeMs);
    }
    writeResponses(m_topics, version, out);
}

} }
