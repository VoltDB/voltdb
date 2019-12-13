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

#include "kipling/messages/SyncGroup.h"

namespace voltdb { namespace kipling {

SyncGroupRequest::SyncGroupRequest(const int16_t version, const NValue& groupId, SerializeInputBE& request) :
        GroupRequest(version, groupId), m_generationId(readInt(request)), m_memberId(readString(request)) {
    if (version >= 3) {
        m_groupInstanceId = readString(request);
    }
    readRequestComponents(version, request, m_assignments);
}

} }
