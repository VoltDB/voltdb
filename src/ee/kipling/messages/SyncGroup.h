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

/**
 * Partition assignment for one of the group members
 */
class MemberAssignment : protected RequestComponent {

public:
    MemberAssignment(const int16_t version, SerializeInputBE &request) :
        m_memberId(readString(request)),
        m_assignement(readBytes(request)) {}

    inline const NValue& memberId() const {
        return m_memberId;
    }

    inline const NValue& assignment() const {
        return m_assignement;
    }

private:
    // Member ID who this assignment is for
    const NValue m_memberId;
    // Partition assignments for the member
    const NValue m_assignement;

};

/**
 * Request to sync group partition assignments
 */
class SyncGroupRequest : public GroupRequest {

public:
    SyncGroupRequest(const int16_t version, const NValue& groupId, SerializeInputBE &request);

    inline const int32_t generationId() const {
        return m_generationId;
    }

    inline const NValue& memberId() const {
        return m_memberId;
    }

    inline const NValue& groupInstanceId() const {
        return m_groupInstanceId;
    }

    inline const std::vector<MemberAssignment>& assignments() const {
        return m_assignments;
    }

private:
    // Generation ID of the group
    const int32_t m_generationId;
    // Member ID for the group
    const NValue m_memberId;
    // Group instance ID if one exists
    NValue m_groupInstanceId;
    // Assignments which are provided by the group leader
    std::vector<MemberAssignment> m_assignments;
};

/**
 * Response to SyncGroupRequest
 */
class SyncGroupResponse : public Response<SyncGroupResponse> {
    SyncGroupResponse() {}
    SyncGroupResponse(Error error) : Response(error) {}

    inline const NValue& assignment() const {
        return m_assignment;
    }

    inline SyncGroupResponse& assignment(const NValue& assignment) {
        m_assignment = assignment;
        return *this;
    }

private:
    // Partition assignments for this member
    NValue m_assignment;
};

} }
