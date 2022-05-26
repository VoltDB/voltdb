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

#include "GroupStore.h"

#include "indexes/tableindex.h"
#include "topics/TableFactory.h"
#include "topics/messages/CheckedSerializeInput.h"
#include "topics/messages/OffsetCommit.h"
#include "topics/messages/OffsetFetch.h"
#include "topics/orm/Group.h"
#include "topics/orm/GroupMember.h"
#include "topics/orm/GroupOffset.h"

namespace voltdb { namespace topics {

#define DECREMENT_REFCOUNT(t) if (t) t->decrementRefcount()

GroupStore::~GroupStore() {
    DECREMENT_REFCOUNT(m_group);
    DECREMENT_REFCOUNT(m_groupMember);
    DECREMENT_REFCOUNT(m_groupOffset);
}

void GroupStore::initialize(VoltDBEngine *engine) {
    initialize(engine->getSystemTable(SystemTableId::TOPICS_GROUP),
            engine->getSystemTable(SystemTableId::TOPICS_GROUP_MEMBER),
            engine->getSystemTable(SystemTableId::TOPICS_GROUP_OFFSET));
}

#define ASSIGN_TABLE(v, t) vassert(t); t->incrementRefcount(); v = t

void GroupStore::initialize(PersistentTable *group, PersistentTable *groupMember, PersistentTable *groupOffset) {
    ASSIGN_TABLE(m_group, group);
    ASSIGN_TABLE(m_groupMember, groupMember);
    ASSIGN_TABLE(m_groupOffset, groupOffset);
}

void GroupStore::storeGroup(SerializeInputBE& groupMetadata) {
    Group::upsert(*this, groupMetadata);
}

void GroupStore::deleteGroup(const NValue& groupId) {
    Group group(*this, groupId);
    group.markForDelete();
    group.commit();

    GroupOffset::deleteIf(*this, groupId, [] (const GroupOffset &offset) { return true; });
}

bool GroupStore::fetchGroups(int maxResultSize, const NValue& startGroupId, SerializeOutput& out) {
    TableTuple next(m_group->schema());

    out.writeVarBinary([this, &startGroupId, maxResultSize, &next] (SerializeOutput& out) {
        TableIndex* index = m_group->primaryKeyIndex();
        TableTuple searchKey(index->getKeySchema());

        char data[searchKey.tupleLength()];
        searchKey.move(data);
        searchKey.setNValue(static_cast<int32_t>(GroupOffsetTable::IndexColumn::GROUP_ID), startGroupId);

        IndexCursor cursor(m_group->schema());
        index->moveToGreaterThanKey(&searchKey, cursor);

        int32_t groupCount = 0;
        size_t pos = out.reserveBytes(sizeof(groupCount));

        while (!(next = index->nextValue(cursor)).isNullTuple()) {
            Group group(*this, next);
            if (out.position() + group.serializedSize() > maxResultSize) {
                break;
            }

            ++groupCount;
            group.serialize(out);
        }

        out.writeIntAt(pos, groupCount);
    });

    return !next.isNullTuple();
}

void GroupStore::commitOffsets(int64_t timestamp, int16_t requestVersion, const NValue& groupId,
        SerializeInputBE& offsets, SerializeOutput& out) {
    OffsetCommitResponse response;
    CheckedSerializeInput checkedIn(offsets);

    Group group(*this, groupId);
    vassert(group.isInTable());
    group.setCommitTimestamp(timestamp);
    group.commit();

    int topicCount = checkedIn.readInt();
    for (int i = 0; i < topicCount; ++i) {
        NValue topic = checkedIn.readString();
        OffsetCommitResponseTopic& responseTopic = response.addTopic(topic);

        int partitionCount = checkedIn.readInt();
        for (int j = 0; j < partitionCount; ++j) {
            OffsetCommitRequestPartition partition(requestVersion, checkedIn);
            GroupOffset offset(*this, groupId, topic, partition.partitionIndex());
            offset.update(partition);
            offset.commit(timestamp);
            responseTopic.addPartition(partition.partitionIndex());
        }
    }

    out.writeVarBinary([&response, requestVersion](SerializeOutput &out) { response.write(requestVersion, out); });
}

void GroupStore::fetchOffsets(int16_t requestVersion, const NValue& groupId, SerializeInputBE& topicPartitions,
        SerializeOutput& out) {
    OffsetFetchResponse response;
    CheckedSerializeInput checkedIn(topicPartitions);

    int topicCount = checkedIn.readInt();
    if (topicCount <= 0) {
        OffsetFetchResponseTopic* responseTopic = nullptr;

        GroupOffset::visitAll(*this, groupId, [&response, &responseTopic] (const GroupOffset& offset) {
            if (responseTopic == nullptr || responseTopic->topic() != offset.getTopic()) {
                responseTopic = &response.addTopic(offset.getTopic());
            }

            responseTopic->addPartition(offset.getPartition(), offset.getOffset(), offset.getLeaderEpoch(),
                    offset.getMetadata());
        });
    } else {
        for (int i = 0; i < topicCount; ++i) {
            NValue topic = checkedIn.readString();
            OffsetFetchResponseTopic& responseTopic = response.addTopic(topic);

            int partitionCount = checkedIn.readInt();
            for (int j = 0; j < partitionCount; ++j) {
                int32_t partition = checkedIn.readInt();
                GroupOffset offset(*this, groupId, topic, partition);

                if (offset.isInTable()) {
                    responseTopic.addPartition(partition, offset.getOffset(), offset.getLeaderEpoch(),
                            offset.getMetadata());
                } else {
                    responseTopic.addPartition(partition, -1, -1, ValueFactory::getNullStringValue());
                }
            }
        }
    }

    out.writeVarBinary([&response, requestVersion](SerializeOutput &out) { response.write(requestVersion, out); });
}


void GroupStore::deleteExpiredOffsets(const int64_t deleteOlderThan) {
    Group::visitStandaloneGroups(*this, [this, &deleteOlderThan] (const NValue& groupId) {
        /* This could be optimized by having an index on (groupId, commitTimestamp) but that would have to index all
         * groups so not sure if that is better or not
         */
        GroupOffset::deleteIf(*this, groupId, [deleteOlderThan] (const GroupOffset& offset) {
            return offset.getCommitTimestamp() < deleteOlderThan;
        });
    });
}

} }
