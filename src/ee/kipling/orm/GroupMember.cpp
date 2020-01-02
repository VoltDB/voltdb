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
#include "kipling/orm/GroupMember.h"

namespace voltdb {
namespace kipling {

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

GroupMember::GroupMember(const GroupTables& tables, const NValue &groupId) :
        GroupOrmBase(tables, groupId), m_memberId(generateMemberId()) {

    setSchema(getTable()->schema());

    initializeValues(-1, -1, ValueFactory::getNullStringValue());
}

void GroupMember::initializeValues(int32_t sessionTimeout, int32_t rebalanceTeimout, const NValue& instanceId) {
    std::vector<NValue> values({
           getGroupId(),
           getMemberId(),
           ValueFactory::getIntegerValue(sessionTimeout),
           ValueFactory::getIntegerValue(rebalanceTeimout),
           instanceId,
           ValueFactory::getNullBinaryValue(),
           ValueFactory::getSmallIntValue(0)
       });

       setNValues(values);
}

bool GroupMember::update(const JoinGroupRequest& request) {
    bool updated = false;

    if (isDeleted()) {
        initializeValues(request.sessionTimeoutMs(), request.rebalanceTimeoutMs(), request.groupInstanceId());
        updated = true;
    } else {
        if (getSessionTimeout() != request.sessionTimeoutMs()) {
            setSessionTimeout(request.sessionTimeoutMs());
            updated = true;
        }
        if (getRebalanceTimeout() != request.rebalanceTimeoutMs()) {
            setRebalanceTimeout(request.rebalanceTimeoutMs());
            updated = true;
        }
        if (getInstanceId() != request.groupInstanceId()) {
            setInstanceId(request.groupInstanceId());
            updated = true;
        }
    }

    // Update protocols
    loadProtocolsIfNecessary();

    // List of current non deleted protocols so we can detect which ones need to be deleted
    std::unordered_set<NValue> currentProtocolNames;
    for (auto iterator = m_protocols.begin(); iterator != m_protocols.end(); ++iterator) {
        if (!iterator->second->isDeleted()) {
            currentProtocolNames.insert(iterator->first);
        }
    }

    // Update or insert protocols from the request
    const std::vector<JoinGroupProtocol>& updatedProtocols = request.protocols();
    for (int i = 0; i < updatedProtocols.size(); ++i) {
        const JoinGroupProtocol& updatedProtocol = updatedProtocols[i];
        GroupMemberProtocol* protocol = getProtocol(updatedProtocol.name());
        if (protocol == nullptr) {
            GroupMemberProtocol *newProtocol = new GroupMemberProtocol(m_tables, getGroupId(), getMemberId(), i,
                    updatedProtocol.name(), updatedProtocol.metadata());
            m_protocols[updatedProtocol.name()].reset(newProtocol);
            updated = true;
        } else {
            currentProtocolNames.erase(protocol->getName());
            updated |= protocol->update(i, updatedProtocol);
        }
    }

    for (auto protocolName : currentProtocolNames) {
        m_protocols[protocolName]->markForDelete();
        updated = true;
    }

    return updated;
}

GroupMemberProtocol* GroupMember::getProtocol(const NValue& protocolName) {
    loadProtocolsIfNecessary();

    auto entry = m_protocols.find(protocolName);
    if (entry != m_protocols.end()) {
        return entry->second.get();
    }
    return nullptr;
}

std::vector<GroupMemberProtocol*> GroupMember::getProtocols(bool includeDeleted) {
    loadProtocolsIfNecessary();

    std::vector<GroupMemberProtocol*> result;

    for (auto iterator = m_protocols.begin(); iterator != m_protocols.end(); ++iterator) {
        if (includeDeleted || !iterator->second->isDeleted()) {
            result.push_back(iterator->second.get());
        }
    }

    return result;
}

void GroupMember::loadProtocolsIfNecessary() {
    if (isInTable() && m_protocols.empty()) {
        for (auto protocol : GroupMemberProtocol::loadProtocols(m_tables, getGroupId(), getMemberId())) {
            m_protocols[protocol->getName()].reset(protocol);
        }
    }
}

void GroupMember::markForDelete() {
    loadProtocolsIfNecessary();
    GroupOrmBase::markForDelete();

    for (auto iterator = m_protocols.begin(); iterator != m_protocols.end(); ++iterator) {
        iterator->second->markForDelete();
    }
}

void GroupMember::commit(int64_t timestamp) {
    GroupOrmBase::commit(timestamp);

    for (auto iterator = m_protocols.begin(); iterator != m_protocols.end(); ++iterator) {
        iterator->second->commit(timestamp);
    }
}

bool GroupMember::equalDeleted(const GroupOrmBase& other) const {
    const GroupMember& otherMember = dynamic_cast<const GroupMember&>(other);
    return getGroupId() == otherMember.getGroupId() && m_memberId == otherMember.m_memberId;
}

} /* namespace kipling */
} /* namespace voltdb */
