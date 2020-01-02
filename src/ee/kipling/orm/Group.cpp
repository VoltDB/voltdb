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
#include "kipling/TableFactory.h"
#include "kipling/orm/Group.h"

namespace voltdb {
namespace kipling {

Group::Group(const GroupTables& tables, const NValue& groupId):
    GroupOrmBase(tables, groupId) {
    TableIndex *index = getTable()->primaryKeyIndex();

    TableTuple searchKey(index->getKeySchema());
    char data[searchKey.tupleLength()];
    searchKey.move(data);

    searchKey.setNValue(static_cast<int32_t>(GroupTable::IndexColumn::ID), groupId);

    IndexCursor cursor(getTable()->schema());
    index->moveToKey(&searchKey, cursor);
    setTableTuple(cursor.m_match);
}

void Group::initializeForInsert() {
    vassert(!isInTable());

    std::vector<NValue> values({
            getGroupId(),
            ValueFactory::getTimestampValue(-1),
            ValueFactory::getIntegerValue(0),
            ValueFactory::getTinyIntValue(static_cast<uint8_t>(GroupState::EMPTY)),
            ValueFactory::getNullBinaryValue(),
            ValueFactory::getNullStringValue()
    });

    setNValues(values);
}

void Group::markForDelete() {
    loadMembersIfNecessary();
    GroupOrmBase::markForDelete();

    for (auto entry = m_members.begin(); entry != m_members.end(); ++entry) {
        entry->second->markForDelete();
    }
}

GroupMember* Group::getMember(const NValue& memberId) {
    loadMembersIfNecessary();

    auto entry = m_members.find(memberId);
    if (entry != m_members.end()) {
        return entry->second.get();
    }

    return nullptr;
}

std::vector<GroupMember*> Group::getMembers(bool includeDeleted) {
    loadMembersIfNecessary();

    std::vector<GroupMember*> result;
    for (auto entry = m_members.begin(); entry != m_members.end(); ++entry) {
        if (includeDeleted || !entry->second->isDeleted()) {
            result.push_back(entry->second.get());
        }
    }
    return result;
}

GroupMember& Group::getOrCreateMember(const NValue& memberId) {
    if (!memberId.isNull()) {
        GroupMember *existing = getMember(memberId);
        if (existing != nullptr) {
            return *existing;
        }
    } else {
        loadMembersIfNecessary();
    }

    GroupMember *newMember = new GroupMember(m_tables, getGroupId());
    m_members[newMember->getMemberId()].reset(newMember);
    return *newMember;
}

void Group::commit(int64_t timestamp) {
    if (isDirty() && !isDeleted()) {
        setNValue(GroupTable::Column::COMMIT_TIMESTAMP, ValueFactory::getTimestampValue(timestamp));
    }
    GroupOrmBase::commit(timestamp);

    for (auto entry = m_members.begin(); entry != m_members.end(); ++entry) {
        entry->second->commit(timestamp);
    }
}


void Group::loadMembersIfNecessary() {
    if (!m_membersLoaded) {
        for (auto member : GroupMember::loadMembers(m_tables, getGroupId())) {
            m_members[member->getMemberId()].reset(member);
        }
        m_membersLoaded = true;
    }
}

bool Group::equalDeleted(const GroupOrmBase& other) const {
    const Group& otherGroup = dynamic_cast<const Group&>(other);
    return getGroupId() == otherGroup.getGroupId();
}

} /* namespace kipling */
} /* namespace voltdb */
