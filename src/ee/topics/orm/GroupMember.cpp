/* This file is part of VoltDB.
 * Copyright (C) 2019-2022 Volt Active Data Inc.
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

#include "indexes/tableindex.h"
#include "topics/TableFactory.h"
#include "topics/orm/GroupMember.h"

namespace voltdb {
namespace topics {

GroupMember::GroupMember(const GroupTables& tables, const NValue& groupId, const NValue& memberId,
        const NValue& clientId, const NValue& clientHost, int32_t sessionTimeout,
        int32_t rebalanceTimeout, const NValue& instanceId, const NValue& protocolMetadata,
        const NValue& assignments) :
        GroupOrmBase(tables, groupId), m_memberId(memberId) {
    setSchema(getTable()->schema());

    std::vector<NValue> values = { groupId, memberId, ValueFactory::getIntegerValue(sessionTimeout),
            ValueFactory::getIntegerValue(rebalanceTimeout), instanceId, protocolMetadata, assignments };

    setNValues(values);
}

std::vector<GroupMember*> GroupMember::loadMembers(const GroupTables& tables, const NValue& groupId) {
    PersistentTable* table = tables.getGroupMemberTable();
    TableIndex* index = table->index(GroupMemberTable::indexName);

    TableTuple searchKey(index->getKeySchema());
    char data[searchKey.tupleLength()];
    searchKey.move(data);

    searchKey.setNValue(static_cast<int32_t>(GroupMemberTable::IndexColumn::GROUP_ID), groupId);

    std::vector<GroupMember*> members;

    IndexCursor cursor(table->schema());
    index->moveToKey(&searchKey, cursor);
    for (; !cursor.m_match.isNullTuple(); index->nextValueAtKey(cursor)) {
        members.emplace_back(new GroupMember(tables, cursor.m_match, groupId));
    }

    return members;
}

GroupMember::GroupMember(const GroupTables& tables, const NValue& groupId, const NValue& memberId) :
        GroupOrmBase(tables, groupId), m_memberId(memberId) {

    setSchema(getTable()->schema());
}

void GroupMember::update(const NValue& clientId, const NValue& clientHost, int32_t sessionTimeout, int32_t rebalanceTimeout,
        const NValue& instanceId, const NValue& protocolMetadata, const NValue& assignments) {
    update(clientId, clientHost, ValueFactory::getIntegerValue(sessionTimeout), ValueFactory::getIntegerValue(rebalanceTimeout),
            instanceId, protocolMetadata, assignments);
}

void GroupMember::update(const NValue& clientId, const NValue& clientHost, const NValue& sessionTimeout, const NValue& rebalanceTimeout,
        const NValue& instanceId, const NValue& protocolMetadata, const NValue& assignments) {
    if (isInTable()) {
        std::vector<NValue> values = { clientId, clientHost, sessionTimeout, rebalanceTimeout, instanceId, protocolMetadata, assignments };
        setNValues(values, GroupMemberTable::Column::CLIENT_ID);
    } else {
        std::vector<NValue> values = { getGroupId(), getMemberId(), clientId, clientHost, sessionTimeout, rebalanceTimeout, instanceId,
                protocolMetadata, assignments };

        setNValues(values);
    }
}

void GroupMember::update(SerializeInputBE& updateIn) {
    NValue clientId = readString(updateIn);
    NValue clientHost = readString(updateIn);
    NValue sessionTimeout = ValueFactory::getIntegerValue(updateIn.readInt());
    NValue rebalanceTimeout = ValueFactory::getIntegerValue(updateIn.readInt());
    NValue instanceId = readString(updateIn);
    NValue protocolMetadata = readBytes(updateIn);
    NValue assignments = readBytes(updateIn);

    update(clientId, clientHost, sessionTimeout, rebalanceTimeout, instanceId, protocolMetadata, assignments);
}

int32_t GroupMember::serializedSize() {
    return getMemberId().serializedSize() + getClientId().serializedSize() + getClientHost().serializedSize()
            + sizeof(int32_t) * 2 + getInstanceId().serializedSize()
            + getProtocolMetadata().serializedSize() + getAssignments().serializedSize();
}

void GroupMember::serialize(SerializeOutput& out) {
    getMemberId().serializeTo(out);
    getClientId().serializeTo(out);
    getClientHost().serializeTo(out);
    out.writeInt(getSessionTimeout());
    out.writeInt(getRebalanceTimeout());
    getInstanceId().serializeTo(out);
    getProtocolMetadata().serializeTo(out);
    getAssignments().serializeTo(out);
}

bool GroupMember::equalDeleted(const GroupOrmBase& other) const {
    const GroupMember& otherMember = dynamic_cast<const GroupMember&>(other);
    return getGroupId() == otherMember.getGroupId() && m_memberId == otherMember.m_memberId;
}

} /* namespace topics */
} /* namespace voltdb */
