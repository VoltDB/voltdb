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

#include <kipling/messages/Error.h>
#include <stdint.h>
#include <string>

#include "common/serializeio.h"
#include "kipling/messages/Message.h"

namespace voltdb { namespace kipling {

// Request classes

class JoinGroupProtocol: protected RequestComponent {

public:
    JoinGroupProtocol(const int16_t version, SerializeInputBE& request);

    // Construct a request by hand. Only really used by tests
    JoinGroupProtocol(const int16_t version, const NValue& name, const NValue& metadata) :
            RequestComponent(), m_name(name), m_metadata(metadata) {}

    inline const NValue& name() const {
        return m_name;
    }

    inline const NValue& metadata() const {
        return m_metadata;
    }

private:
    // Name of partition selection protocol
    const NValue m_name;
    // Metadata associated with this protocol
    const NValue m_metadata;
};

/**
 * Request to join a group sent on initial join or after a rebalance has been triggered
 */
class JoinGroupRequest: public GroupRequest {

public:
    JoinGroupRequest(const int16_t version, const NValue& groupId, SerializeInputBE& request);

    // Construct a request by hand. Only really used by tests
    JoinGroupRequest(const int16_t version, const NValue& groupId, const NValue& memberId, int32_t sessionTimeoutMs,
            int32_t rebalanceTimeoutMs, const NValue& groupInstanceId, std::vector<JoinGroupProtocol>& protocols) :
            GroupRequest(version, groupId), m_sessionTimeoutMs(sessionTimeoutMs),
            m_rebalanceTimeoutMs(rebalanceTimeoutMs), m_memberId(memberId), m_groupInstanceId(groupInstanceId),
            m_protocols(protocols) {}

    inline const int32_t sessionTimeoutMs() const {
        return m_sessionTimeoutMs;
    }

    inline const int32_t rebalanceTimeoutMs() const {
        return m_rebalanceTimeoutMs;
    }

    inline const NValue& memberId() const {
        return m_memberId;
    }

    inline const NValue& groupInstanceId() const {
        return m_groupInstanceId;
    }

    inline const NValue& protocolType() const {
        return m_protocolType;
    }

    inline const std::vector<JoinGroupProtocol>& protocols() const {
        return m_protocols;
    }

private:
    // Heartbeat timeout for this joining member
    int32_t m_sessionTimeoutMs;
    // Partition rebalance for this joining member. The max of all member timeouts is used for the group
    int32_t m_rebalanceTimeoutMs;
    // ID of this member if it has one
    NValue m_memberId;
    // Optional group instance ID for this joining member
    NValue m_groupInstanceId;
    // Protocol used by this joining member. Should always be CONSUMER
    NValue m_protocolType;
    // List of partition selection protocols supported by this member
    std::vector<JoinGroupProtocol> m_protocols;
};

// Response classes

/**
 * Response component describing one of the members of the group
 */
class JoinGroupMember: protected ResponseComponent {

public:
    JoinGroupMember(const NValue& memberId, const NValue& groupInstanceId, const NValue& metadata) :
            m_memberId(memberId), m_groupInstanceId(groupInstanceId), m_metadata(metadata) {}

    void write(const int16_t version, SerializeOutput& out) const override;

    inline const NValue& memberId() const {
        return m_memberId;
    }

    inline const NValue& groupInstanceId() const {
        return m_groupInstanceId;
    }

    inline const NValue& metadata() const {
        return m_metadata;
    }

private:
    // ID of the group member
    const NValue m_memberId;
    // Group instance ID if exists of this member
    const NValue m_groupInstanceId;
    // Partition selection protocol for this member
    const NValue m_metadata;
};

/**
 * Response to JoinGroupRequest
 */
class JoinGroupResponse: public Response<JoinGroupResponse> {

public:
    JoinGroupResponse() {}
    JoinGroupResponse(Error error) : Response(error) {}

    void write(const int16_t version, SerializeOutput& out) const override;

    inline const int32_t generationId() const {
        return m_generationId;
    }

    inline JoinGroupResponse& generationId(int32_t generationId) {
        m_generationId = generationId;
        return *this;
    }

    inline const NValue& protocolName() const {
        return m_protocolName;
    }

    inline JoinGroupResponse& protocolName(const NValue& protocolName) {
        m_protocolName = protocolName;
        return *this;
    }

    inline const NValue& leader() const {
        return m_leader;
    }

    inline JoinGroupResponse& leader(const NValue& leader) {
        m_leader = leader;
        return *this;
    }

    inline const NValue& memberId() const {
        return m_memberId;
    }

    inline JoinGroupResponse& memberId(const NValue& memberId) {
        m_memberId = memberId;
        return *this;
    }

    inline const std::vector<JoinGroupMember>& members() const {
        return m_members;
    }

    template <typename... Args>
    inline JoinGroupMember& addMember(Args&&... args) {
        m_members.emplace_back(std::forward<Args>(args)...);
        return m_members.back();
    }

private:
    // Generation ID of the group
    int32_t m_generationId = -1;
    // Partition assignment protocol selected by server
    NValue m_protocolName;
    // Member ID of leader selected to perform partition assignment
    NValue m_leader;
    // ID of this joining member
    NValue m_memberId;
    // All group members and their associated protocol metadata. This is only populated when sent to leader
    std::vector<JoinGroupMember> m_members;
};

} }
