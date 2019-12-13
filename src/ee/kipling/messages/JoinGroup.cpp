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

#include "JoinGroup.h"

namespace voltdb { namespace kipling {

JoinGroupProtocol::JoinGroupProtocol(const int16_t version, SerializeInputBE& request) :
        m_name(readString(request)),
        m_metadata(readBytes(request)) {}

JoinGroupRequest::JoinGroupRequest(const int16_t version, const NValue& groupId, SerializeInputBE& request) :
        GroupRequest(version, groupId) {
    m_sessionTimeoutMs = request.readInt();
    if (version >= 1) {
        m_rebalanceTimeoutMs = request.readInt();
    } else {
        m_rebalanceTimeoutMs = -1;
    }
    m_memberId = readString(request);
    if (version >= 5) {
        m_groupInstanceId = readString(request);
    }
    m_protocolType = readString(request);
    readRequestComponents(version, request, m_protocols);
}

void JoinGroupMember::write(const int16_t version, SerializeOutput& out) const {
    writeString(m_memberId, out);
    if (version >= 5) {
        writeString(m_groupInstanceId, out);
    }
    writeBytes(m_metadata, out);
}

void JoinGroupResponse::write(const int16_t version, SerializeOutput& out) const {
    writeCommon(2, version, out);
    out.writeInt(m_generationId);
    writeString(m_protocolName, out);
    writeString(m_leader, out);
    writeString(m_memberId, out);
    writeResponses(m_members, version, out);
}

} }
