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
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "kipling/messages/JoinGroup.h"
#include "kipling/orm/GroupOrmBase.h"
#include "kipling/orm/GroupMemberProtocol.h"
#include "storage/persistenttable.h"

namespace voltdb {
namespace kipling {

/**
 * Class which represents a row from GroupMemberTable. The row represents a single member of a group
 */
class GroupMember: public GroupOrmBase {
    friend class Group;

public:
    /**
     * Update this group member with information from a join group request
     * @param request: JoinGroupRequest with member information
     * @return true if this or any protocols were updated
     */
    bool update(const JoinGroupRequest& request);

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
     * @return the assignments for this group member
     */
    const NValue getAssignments() const {
        return getNValue(GroupMemberTable::Column::ASSIGNMENTS);
    }

    void setAssignments(const NValue& assignments) {
        setNValue(GroupMemberTable::Column::ASSIGNMENTS, assignments);
    }

    /**
     * Delete this group member and all protocols which are associated with this member
     */
    void markForDelete() override;

    /**
     * @param protocolName: Name of protocol to return
     * @return the protocol with the given name or nullptr if the protocol does not exist
     */
    GroupMemberProtocol* getProtocol(const NValue& protocolName);

    /**
     * @param includeDeleted: If true then all protocols including deleted protocols are returned. Default false
     */
    std::vector<GroupMemberProtocol*> getProtocols(bool includeDeleted = false);

protected:
    /**
     * Load all members of a group from the members table
     */
    static std::vector<GroupMember*> loadMembers(const GroupTables& tables, const NValue& groupId);

    GroupMember(const GroupTables& tables, const NValue& groupId);

    void commit(int64_t timestamp) override;

    bool equalDeleted(const GroupOrmBase& other) const override;

    PersistentTable* getTable() const override { return m_tables.getGroupMemberTable(); }

private:
    /**
     * Generate a new member ID from a UUID as a string
     */
    static NValue generateMemberId() {
        boost::uuids::random_generator generator;
        boost::uuids::uuid memberUuid(generator());
        std::string memberId = boost::uuids::to_string(memberUuid);
        return ValueFactory::getTempStringValue(memberId);
    }

    GroupMember(const GroupTables& tables, TableTuple& original, const NValue& groupId) :
        GroupOrmBase(tables, original, groupId), m_memberId(getNValue(GroupMemberTable::Column::MEMBER_ID)) {}

    void initializeValues(int32_t sessionTimeout, int32_t rebalanceTeimout, const NValue& instanceId);

    void setSessionTimeout(int32_t timeout) {
        setNValue(GroupMemberTable::Column::SESSION_TIMEOUT, ValueFactory::getIntegerValue(timeout));
    }

    void setRebalanceTimeout(int32_t timeout) {
        setNValue(GroupMemberTable::Column::REBALANCE_TIMEOUT, ValueFactory::getIntegerValue(timeout));
    }

    void setInstanceId(const NValue& instanceId) {
        setNValue(GroupMemberTable::Column::INSTANCE_ID, instanceId);
    }

    const int16_t getFlags() const {
        return ValuePeeker::peekSmallInt(getNValue(GroupMemberTable::Column::FLAGS));
    }

    void setFlags(int16_t flags) {
        setNValue(GroupMemberTable::Column::FLAGS, ValueFactory::getSmallIntValue(flags));
    }

    /**
     * Load all assignment protocols supported by this member if not already loaded
     */
    void loadProtocolsIfNecessary();

    // this group members ID. Type VARCHAR
    const NValue m_memberId;
    // map from protocol name to protocol object supported by this group member
    std::unordered_map<NValue, std::unique_ptr<GroupMemberProtocol>> m_protocols;
};

} /* namespace kipling */
} /* namespace voltdb */
