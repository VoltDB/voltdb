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

#pragma once

#include <stdint.h>
#include <string>

#include "common/serializeio.h"
#include "kipling/messages/Error.h"
#include "kipling/messages/Message.h"

namespace voltdb { namespace kipling {

class LeaveGroupRequest : public GroupRequest {

public:
    LeaveGroupRequest(const int16_t version, const NValue& groupId, SerializeInputBE& request) :
        GroupRequest(version, groupId),
        m_memberId(readString(request)) {}

    inline const NValue& memberId() {
        return m_memberId;
    }

private:
    // Member ID of member leaving the group
    const NValue m_memberId;
};

class LeaveGroupResponse : public Response<LeaveGroupResponse> {

public:
    LeaveGroupResponse(Error error) : Response(error) {}

    void write(const int16_t version, SerializeOutput &out) const {
        writeCommon(1, version, out);
    }
};

} }
