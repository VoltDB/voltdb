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

#include "topics/messages/OffsetFetch.h"

namespace voltdb { namespace topics {

OffsetFetchResponsePartition::OffsetFetchResponsePartition(int16_t version, CheckedSerializeInput& in) :
        m_partitionIndex(in.readInt()), m_offset(in.readLong()) {
    if (version >= 5) {
        m_leaderEpoch = in.readInt();
    }
    m_metadata = in.readString();
    __attribute__((unused)) int16_t error = in.readShort();
    vassert(error == 0);
}

void OffsetFetchResponsePartition::write(const int16_t version, SerializeOutput& out) const {
    out.writeInt(m_partitionIndex);
    out.writeLong(m_offset);
    if (version >= 5) {
        out.writeInt(m_leaderEpoch);
    }
    writeString(m_metadata, out, true);
    writeError(out);
}

OffsetFetchResponseTopic::OffsetFetchResponseTopic(int16_t version, CheckedSerializeInput& in) :
        m_topic(in.readString()) {
    in.readComponents(version, m_partitions);
}

void OffsetFetchResponseTopic::write(const int16_t version, SerializeOutput& out) const {
    writeString(m_topic, out);
    writeResponses(m_partitions, version, out);
}

OffsetFetchResponse::OffsetFetchResponse(int16_t version, CheckedSerializeInput& in) {
    in.readComponents(version, m_topics);
    if (version >= 2) {
        __attribute__((unused)) int16_t error = in.readShort();
        vassert(error == 0);
    }
}

void OffsetFetchResponse::write(const int16_t version, SerializeOutput& out) const {
    writeResponses(m_topics, version, out);
    if (version >= 2) {
        writeError(out);
    }
}

} }
