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
#include <functional>

#include "kipling/TableFactory.h"
#include "kipling/orm/GroupOrmBase.h"
#include "storage/persistenttable.h"

namespace voltdb {
namespace kipling {

/**
 * Class which represents a row from GroupMemberTable. The row represents a single member of a group
 */
class GroupMember: public GroupOrmBase {
    friend class Group;

public:
    GroupMember(const GroupTables& tables, const NValue& groupId, const NValue& memberId, int32_t sessionTimeout,
            int32_t rebalanceTimeout, const NValue& instanceId, const NValue& protocolMetadata,
            const NValue& assignments);

    /**
     * @return the ID of this group member. Type VARCHAR
     */
    const NValue& getMemberId() const {
        return m_memberId;
    }

    /**
     * @return the session timeout for heartbeating
     */
    const int32_t getSessionTimeout() const {
        return ValuePeeker::peekInteger(getNValue(GroupMemberTable::Column::SESSION_TIMEOUT));
    }

    /**
     * @return the rebalance timeout
     */
    const int32_t getRebalanceTimeout() const {
        return ValuePeeker::peekInteger(getNValue(GroupMemberTable::Column::REBALANCE_TIMEOUT));
    }

    /**
     * @return client provided group instance ID for persistent group membership
     */
    const NValue getInstanceId() const {
        return getNValue(GroupMemberTable::Column::INSTANCE_ID);
    }

    /**
     * @return the members metadata for the selected assignment protocol
     */
    const NValue getProtocolMetadata() const {
        return getNValue(GroupMemberTable::Column::PROTOCOL_METADATA);
    }

    /**
     * @return the assignments for this group member
     */
    const NValue getAssignments() const {
        return getNValue(GroupMemberTable::Column::ASSIGNMENTS);
    }

    /**
     * Update used by tests to set all fields in this member
     */
    void update(int32_t sessionTimeout, int32_t rebalanceTimeout, const NValue& instanceId,
            const NValue& protocolMetadata, const NValue& assignments);

protected:
    /**
     * Load all members of a group from the members table
     */
    static std::vector<GroupMember*> loadMembers(const GroupTables& tables, const NValue& groupId);

    GroupMember(const GroupTables& tables, const NValue& groupId, const NValue& memberId);

    /**
     * Update this group member with information from a join group request
     * @param updateIn: SerializeInputBE with member information
     */
    void update(SerializeInputBE& updateIn);

    /**
     * Size of this group member when serialized
     */
    int32_t serializedSize();

    /**
     * Serialize group member to out
     */
    void serialize(SerializeOutput& out);

    bool equalDeleted(const GroupOrmBase& other) const override;

    PersistentTable* getTable() const override { return m_tables.getGroupMemberTable(); }

private:

    GroupMember(const GroupTables& tables, TableTuple& original, const NValue& groupId) :
        GroupOrmBase(tables, original, groupId), m_memberId(getNValue(GroupMemberTable::Column::MEMBER_ID)) {}

    void update(const NValue& sessionTimeout, const NValue& rebalanceTimeout, const NValue& instanceId,
            const NValue& protocolMetadata, const NValue& assignments);

    // this group members ID. Type VARCHAR
    const NValue m_memberId;
};

} /* namespace kipling */
} /* namespace voltdb */
