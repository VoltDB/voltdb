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

#include "kipling/messages/OffsetFetch.h"

namespace voltdb { namespace kipling {

OffsetFetchRequestTopic::OffsetFetchRequestTopic(const int16_t version, SerializeInputBE& request) : m_topic(readString(request)) {
    const int32_t partitionCount = readInt(request);
    for (int i = 0; i < partitionCount; ++i) {
        m_partitions.push_back(readInt(request));
    }
}

OffsetFetchRequest::OffsetFetchRequest(const int16_t version, const NValue& groupId, SerializeInputBE& request) :
        GroupRequest(version, groupId) {
    readRequestComponents(version, request, m_topics);
}

void OffsetFetchResponsePartition::write(const int16_t version, SerializeOutput& out) const {
    out.writeInt(m_partitionIndex);
    out.writeLong(m_offset);
    if (version >= 5) {
        out.writeInt(m_leaderEpoch);
    }
    writeString(m_metadata, out);
    writeError(m_error, out);
}

void OffsetFetchResponseTopic::write(const int16_t version, SerializeOutput& out) const {
    writeString(m_topic, out);
    writeResponses(m_partitions, version, out);
}

void OffsetFetchResponse::write(const int16_t version, SerializeOutput& out) const {
    if (version >= 3) {
        out.writeInt(throttleTimeMs());
    }
    writeResponses(m_topics, version, out);
    if (version >= 2) {
        writeError(error(), out);
    }
}

} }
