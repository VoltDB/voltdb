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

#include "indexes/tableindex.h"
#include "topics/TableFactory.h"
#include "topics/orm/Group.h"

namespace voltdb {
namespace topics {

void Group::upsert(const GroupTables& tables, SerializeInputBE& groupMetadata) {
    NValue groupId = readString(groupMetadata);
    Group group(tables, groupId);
    group.update(groupMetadata);

    // Mark all current members deleted so that if the member is not in the update it will be deleted
    group.visitMembers([] (GroupMember& member) { member.markForDelete(); });

    int16_t memberCount = groupMetadata.readInt();
    for (int16_t i = 0; i < memberCount; ++i) {
        group.getOrCreateMember(readString(groupMetadata)).update(groupMetadata);
    }

    group.commit();
}

void Group::visitStandaloneGroups(const GroupTables& tables, std::function<void(const NValue&)> visitor) {
    PersistentTable* table = tables.getGroupTable();
    TableIndex* index = table->index(GroupTable::standaloneGroupIndexName);

    IndexCursor cursor(table->schema());
    index->moveToEnd(true, cursor);
    TableTuple tuple(table->schema());

    while (!(tuple = index->nextValue(cursor)).isNullTuple()) {
        NValue groupId = tuple.getNValue(static_cast<int32_t>(GroupTable::Column::ID));
        visitor(groupId);
    }
}

Group::Group(const GroupTables& tables, TableTuple& tuple) :
        GroupOrmBase(tables, tuple, tuple.getNValue(static_cast<int32_t>(GroupTable::Column::ID))) {}

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

Group::Group(const GroupTables& tables, const NValue& groupId, int64_t timestamp, int32_t generation,
        const NValue& leader, const NValue& protocol) :
        GroupOrmBase(tables, groupId) {

    setSchema(getTable()->schema());

    std::vector<NValue> values = { groupId, ValueFactory::getTimestampValue(timestamp),
            ValueFactory::getIntegerValue(generation), leader, protocol };

    setNValues(values);
}

void Group::markForDelete() {
    loadMembersIfNecessary();
    GroupOrmBase::markForDelete();

    for (auto& member : m_members) {
        member.second->markForDelete();
    }
}

GroupMember* Group::getMember(const NValue& memberId, bool includeDeleted) {
    loadMembersIfNecessary();

    auto entry = m_members.find(memberId);
    if (entry != m_members.end() && (includeDeleted || !entry->second->isDeleted())) {
        return entry->second.get();
    }

    return nullptr;
}

std::vector<GroupMember*> Group::getMembers(bool includeDeleted) {
    std::vector<GroupMember*> result;
    visitMembers([&result] (GroupMember& member) mutable { result.push_back(&member); }, includeDeleted);
    return result;
}

void Group::visitMembers(std::function<void(GroupMember&)> visitor, bool includeDeleted) {
    loadMembersIfNecessary();
    for (auto& member : m_members) {
        if (includeDeleted || !member.second->isDeleted()) {
            visitor(*member.second.get());
        }
    }
}

GroupMember& Group::getOrCreateMember(const NValue& memberId) {
    GroupMember *existing = getMember(memberId, true);
    if (existing != nullptr) {
        return *existing;
    }

    GroupMember *newMember = new GroupMember(m_tables, getGroupId(), memberId);
    m_members[newMember->getMemberId()].reset(newMember);
    return *newMember;
}

bool Group::hasMember(bool includeDeleted) {
    loadMembersIfNecessary();

    for (auto& member : m_members) {
        if (includeDeleted || !member.second->isDeleted()) {
            return true;
        }
    }
    return false;
}

void Group::commit(int64_t timestamp) {
    if (willUpdate()) {
        // This really shouldn't happen but just in case handling transitions to and from standalone group
        TableIndex* index = getTable()->index(GroupTable::standaloneGroupIndexName);
        if (index->checkForIndexChange(&getUpdateTuple(), &getTableTuple())) {
            addUpdatedIndex(index);
        }
    }
    GroupOrmBase::commit(timestamp);

    for (auto& member : m_members) {
        member.second->commit(timestamp);
    }
}

int32_t Group::serializedSize() {
    int size = getGroupId().serializedSize();
    size += sizeof(int64_t) + sizeof(int32_t);
    size += getLeader().serializedSize();
    size += getProtocol().serializedSize() + sizeof(int32_t);
    visitMembers([&size] (GroupMember& member) { size += member.serializedSize(); });
    return size;
}

void Group::serialize(SerializeOutput& out) {
    loadMembersIfNecessary();

    getGroupId().serializeTo(out);
    out.writeLong(getCommitTimestamp());
    out.writeInt(getGeneration());
    getLeader().serializeTo(out);
    getProtocol().serializeTo(out);

    size_t memberCountPos = out.reserveBytes(sizeof(int32_t));
    int32_t memberCount = 0;
    visitMembers([&memberCount, &out] (GroupMember &member) {
        ++memberCount;
        member.serialize(out);
    });
    out.writeIntAt(memberCountPos, memberCount);
}

void Group::update(SerializeInputBE& updateIn) {
    NValue timestamp = ValueFactory::getTimestampValue(updateIn.readLong());
    NValue generation = ValueFactory::getIntegerValue(updateIn.readInt());
    NValue leader = readString(updateIn);
    NValue protocol = readString(updateIn);

    if (isInTable()) {
        std::vector<NValue> values = { timestamp, generation, leader, protocol };

        setNValues(values, GroupTable::Column::COMMIT_TIMESTAMP);
    } else {
        std::vector<NValue> values = { getGroupId(), timestamp, generation, leader, protocol };

        setNValues(values);
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

} /* namespace topics */
} /* namespace voltdb */
