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

#include "kipling/TableFactory.h"
#include "kipling/messages/JoinGroup.h"
#include "kipling/orm/GroupOrmBase.h"

namespace voltdb {
namespace kipling {

/**
 * Class which represents a row from GroupMemberProtocolTable. The data represents a partition assignment protocol
 * supported by this a group member and the metadata which is used to perform partition assignments
 */
class GroupMemberProtocol: public GroupOrmBase {
    friend class GroupMember;
public:
    /**
     * @return the member ID of the group member for which this protocol
     */
    const NValue& getMemberId() const {
        return m_memberId;
    }

    /**
     * @return the index of this protocol in the list of protocols supported by the group member
     */
    const int16_t getIndex() const {
        return ValuePeeker::peekSmallInt(getNValue(GroupMemberProtocolTable::Column::INDEX));
    }

    /**
     * @return the name of the protocol
     */
    const NValue& getName() const {
        return m_name;
    }

    /**
     * @return the metadata for this protocol for this group member
     */
    const NValue getMetadata() const {
        return getNValue(GroupMemberProtocolTable::Column::METADATA);
    }

protected:
    /**
     * Load protocols from the protocol table for the given group and member
     */
    static std::vector<GroupMemberProtocol*> loadProtocols(const GroupTables& tables, const NValue& groupId,
            const NValue& memberId);

    GroupMemberProtocol(const GroupTables& tables, const NValue& groupId, const NValue& memberId, int16_t index,
            const NValue& name, const NValue& metadata);

    GroupMemberProtocol(const GroupTables& tables, TableTuple& original, const NValue& groupId, const NValue& memberId);

    /**
     * Update the protocol information with the given index and metadata from request.
     * @return true if this protocol was changed
     */
    bool update(int index, const JoinGroupProtocol& request);

    bool equalDeleted(const GroupOrmBase& other) const override;

    PersistentTable* getTable() const override { return m_tables.getGroupMemberProtocolTable(); }

private:
    void initializeValues(int16_t index, const NValue& metadata);

    void setIndex(int16_t index) {
        setNValue(GroupMemberProtocolTable::Column::INDEX, ValueFactory::getSmallIntValue(index));
    }

    void setMetadata(const NValue& metadata) {
        setNValue(GroupMemberProtocolTable::Column::METADATA, metadata);
    }

    const NValue& m_memberId;
    const NValue m_name;
};

} /* namespace kipling */
} /* namespace voltdb */
