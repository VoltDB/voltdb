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

#include "indexes/tableindex.h"
#include "kipling/orm/GroupMemberProtocol.h"
#include "storage/persistenttable.h"

namespace voltdb {
namespace kipling {

std::vector<GroupMemberProtocol*> GroupMemberProtocol::loadProtocols(const GroupTables& tables, const NValue& groupId,
        const NValue& memberId) {
    PersistentTable* table = tables.getGroupMemberProtocolTable();
    TableIndex* index = table->index(GroupMemberProtocolTable::indexName);

    TableTuple searchKey(index->getKeySchema());
    char data[searchKey.tupleLength()];
    searchKey.move(data);

    searchKey.setNValue(static_cast<int32_t>(GroupMemberProtocolTable::IndexColumn::GROUP_ID), groupId);
    searchKey.setNValue(static_cast<int32_t>(GroupMemberProtocolTable::IndexColumn::MEMBER_ID), memberId);

    std::vector<GroupMemberProtocol*> protocols;

    IndexCursor cursor(table->schema());
    index->moveToKey(&searchKey, cursor);
    for (; !cursor.m_match.isNullTuple(); index->nextValueAtKey(cursor)) {
        protocols.emplace_back(new GroupMemberProtocol(tables, cursor.m_match, groupId, memberId));
    }
    return protocols;
}

GroupMemberProtocol::GroupMemberProtocol(const GroupTables& tables, const NValue& groupId, const NValue& memberId,
        int16_t index, const NValue& name, const NValue& metadata) :
        GroupOrmBase(tables, groupId), m_memberId(memberId), m_name(name) {

    setSchema(getTable()->schema());

    initializeValues(index, metadata);
}

GroupMemberProtocol::GroupMemberProtocol(const GroupTables& tables, TableTuple &original, const NValue &groupId,
        const NValue &memberId) :
        GroupOrmBase(tables, original, groupId), m_memberId(memberId),
        m_name(getNValue(GroupMemberProtocolTable::Column::NAME)) {}

void GroupMemberProtocol::initializeValues(int16_t index, const NValue& metadata) {
    std::vector<NValue> values({
                getGroupId(),
                getMemberId(),
                ValueFactory::getSmallIntValue(index),
                getName(),
                metadata
        });

        setNValues(values);
}

bool GroupMemberProtocol::update(int index, const JoinGroupProtocol& request) {
    vassert(getName() == request.name());
    bool updated = false;

    if (isDeleted()) {
        initializeValues(index, request.metadata());
        updated = true;
    } else {
        if (getIndex() != index) {
            setIndex(index);
            updated = true;
        }

        if (getMetadata() != request.metadata()) {
            setMetadata(request.metadata());
            updated = true;
        }
    }

    return updated;
}

bool GroupMemberProtocol::equalDeleted(const GroupOrmBase &other) const {
    const GroupMemberProtocol &otherProtocol = dynamic_cast<const GroupMemberProtocol&>(other);
    return getGroupId() == otherProtocol.getGroupId() && getMemberId() == otherProtocol.getMemberId()
            && getName() == otherProtocol.getName();
}

} /* namespace kipling */
} /* namespace voltdb */
