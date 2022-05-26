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

#pragma once

#include <functional>
#include <unordered_map>

#include "topics/orm/GroupOrmBase.h"
#include "topics/orm/GroupMember.h"

namespace voltdb {
namespace topics {

/**
 * Class which represents a row from GroupTable. The row represents the state of a topics group
 */
class Group : public GroupOrmBase {
public:
    static void upsert(const GroupTables& tables, SerializeInputBE& group);

    /**
     * Visit all groups which are "standalone" groups. That is groups which are only storing offsets in the system
     * tables and will never have any members.
     *
     * @param visitor which will be passed the groupId for each standalone group
     */
    static void visitStandaloneGroups(const GroupTables& tables, std::function<void(const NValue&)> visitor);

    Group(const GroupTables& tables, TableTuple& tuple);

    Group(const GroupTables& tables, const NValue& groupId);

    Group(const GroupTables& tables, const NValue& groupId, int64_t timestamp, int32_t generation,
            const NValue& leader, const NValue& protocol);

    /**
     * Returns the timestamp for when this group was last committed or -1 if this group was never committed
     */
    const int64_t getCommitTimestamp() const {
        return ValuePeeker::peekTimestamp(getNValue(GroupTable::Column::COMMIT_TIMESTAMP));
    }

    void setCommitTimestamp(int64_t timestamp) {
        setNValue(GroupTable::Column::COMMIT_TIMESTAMP, ValueFactory::getTimestampValue(timestamp));
    }

    /**
     * Returns the current group generation
     */
    const int32_t getGeneration() const {
        return ValuePeeker::peekAsInteger(getNValue(GroupTable::Column::GENERATION));
    }

    /**
     * Get the member ID of the current group leader. Type VARCHAR
     */
    const NValue getLeader() const {
        return getNValue(GroupTable::Column::LEADER);
    }

    /**
     * Get the name of the selected partition assignment protocol. Type VARCHAR
     */
    const NValue getProtocol() const {
        return getNValue(GroupTable::Column::PROTOCOL);
    }

    /**
     * Mark this group and group members for delete. Only works if group members are already loaded
     */
    void markForDelete() override;

    /**
     * @param memberId: ID of member to return
     * @return the group member with the given member ID or nullptr if the member does not exist
     */
    GroupMember* getMember(const NValue& memberId, bool includeDeleted = false);

    /**
     * Visit members of the group and pass the member to the visitor
     */
    void visitMembers(std::function<void(GroupMember&)> visitor, bool includeDeleted = false);

    /**
     * @param includeDeleted: If true then all members including deleted members are returned. Default false
     */
    std::vector<GroupMember*> getMembers(bool includeDeleted = false);

    /**
     * @param memberId: ID of member to return
     * @return the group member with the given member ID or create a new member with a new ID
     */
    GroupMember& getOrCreateMember(const NValue& memberId);

    void commit() {
        commit(0);
    }

    /**
     * @return true if there is at least one member of this group
     */
    bool hasMember(bool includeDeleted = false);

    /**
     * Size of data that would be written when serialize is invoked
     */
    int32_t serializedSize();

    /**
     * Serialize this group and all members to out
     */
    void serialize(SerializeOutput& out);

protected:

    virtual void commit(int64_t timestamp) override;

    PersistentTable* getTable() const override { return m_tables.getGroupTable(); }

    bool equalDeleted(const GroupOrmBase& other) const override;

private:

    void update(SerializeInputBE& updateIn);

    /**
     * Load group members if not already loaded
     */
    void loadMembersIfNecessary();

    // A map of memberId to member instance
    std::unordered_map<NValue, std::unique_ptr<GroupMember>> m_members;
    // Whether or not members have been loaded
    bool m_membersLoaded = false;
};

} /* namespace topics */
} /* namespace voltdb */
