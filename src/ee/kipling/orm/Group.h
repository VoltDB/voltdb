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

#include <unordered_map>

#include "kipling/orm/GroupOrmBase.h"
#include "kipling/orm/GroupMember.h"

namespace voltdb {
namespace kipling {

enum class GroupState : int8_t {
    EMPTY,
    REBALNCE_PENDING,
    REBALANCE_COMPLETE,
    STABLE
};

/**
 * Class which represents a row from GroupTable. The row represents the state of a kipling group
 */
class Group : public GroupOrmBase {
public:
    Group(const GroupTables& tables, const NValue& groupId);

    /**
     * Returns the timestmap for when this group was last committed or -1 if this group was never committed
     */
    const int64_t getCommitTimestamp() const {
        return isInTable() ? ValuePeeker::peekTimestamp(getNValue(GroupTable::Column::COMMIT_TIMESTAMP)) : -1;
    }

    /**
     * Returns the current group generation
     */
    const int32_t getGeneration() const {
        return ValuePeeker::peekAsInteger(getNValue(GroupTable::Column::GENERATION));
    }

    /**
     * Increments the group generation by 1
     */
    void incrementGeneration() {
        NValue value = ValueFactory::getIntegerValue(getGeneration() + 1);
        setNValue(GroupTable::Column::GENERATION, value);
    }

    /**
     * Returns the current state of this group
     */
    const GroupState getState() const {
        int8_t state = ValuePeeker::peekTinyInt(getNValue(GroupTable::Column::STATE));
        return static_cast<GroupState>(state);
    }

    /**
     * Sets the state of this group
     */
    void setState(GroupState state) {
        setNValue(GroupTable::Column::STATE, ValueFactory::getTinyIntValue(static_cast<int8_t>(state)));
    }

    /**
     * Get the member ID of the current group leader. Type VARCHAR
     */
    const NValue getLeader() const {
        return getNValue(GroupTable::Column::LEADER);
    }

    /**
     * Set the member ID who is the current group leader
     */
    void setLeader(const NValue& value) {
        setNValue(GroupTable::Column::LEADER, value);
    }

    /**
     * Get the name of the selected partition assignment protocol. Type VARCHAR
     */
    const NValue getProtocol() const {
        return getNValue(GroupTable::Column::PROTOCOL);
    }

    /**
     * Set the name of the selected partition assignment protocol
     */
    void setProtocol(const NValue& value) {
        setNValue(GroupTable::Column::PROTOCOL, value);
    }

    /**
     * Initialize this tuple for insert into the table. isInTable() must return false
     */
    void initializeForInsert();

    /**
     * Mark this group and group members for delete. Only works if group members are already loaded
     */
    void markForDelete() override;

    /**
     * @param memberId: ID of member to return
     * @return the group member with the given member ID or nullptr if the member does not exist
     */
    GroupMember* getMember(const NValue& memberId);

    /**
     * @param includeDeleted: If true then all members including deleted members are returned. Default false
     */
    std::vector<GroupMember*> getMembers(bool includeDeleted = false);

    /**
     * @param memberId: ID of member to return
     * @return the group member with the given member ID or create a new member with a new ID
     */
    GroupMember& getOrCreateMember(const NValue& memberId);

    bool equalDeleted(const GroupOrmBase& other) const override;

    virtual void commit(int64_t timestamp) override;

protected:
    PersistentTable* getTable() const override { return m_tables.getGroupTable(); }

private:
    /**
     * Load group members if not already loaded
     */
    void loadMembersIfNecessary();

    // A map of memberId to member instance
    std::unordered_map<NValue, std::unique_ptr<GroupMember>> m_members;
    // Whether or not members have been loaded
    bool m_membersLoaded = false;
};

} /* namespace kipling */
} /* namespace voltdb */
