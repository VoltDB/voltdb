/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

#include "topics/messages/OffsetCommit.h"

namespace voltdb { namespace topics {

OffsetCommitRequestPartition::OffsetCommitRequestPartition(const int16_t version, CheckedSerializeInput& request) :
        m_partitionIndex(request.readInt()), m_offset(request.readLong()) {
    if (version >= 6) {
        m_leaderEpoch = request.readInt();
    }
    if (version == 1) {
        m_timestamp = request.readLong();
    }
    m_metadata = request.readString();
}

void OffsetCommitResponsePartition::write(const int16_t version, SerializeOutput& out) const {
    out.writeInt(m_partitionIndex);
    writeError(out);
}

void OffsetCommitResponseTopic::write(const int16_t version, SerializeOutput& out) const {
    writeString(m_topic, out);
    writeResponses(m_partitions, version, out);
}

void OffsetCommitResponse::write(const int16_t version, SerializeOutput& out) const {
    writeResponses(m_topics, version, out);
}

} }
