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
#include "topics/orm/GroupOffset.h"

namespace voltdb {
namespace topics {

/*
 * Internal visit which visits the tuples of all offsets for the given groupId
 */
void GroupOffset::visitAll(const GroupTables& tables, const NValue& groupId, std::function<void(TableTuple&)> visitor) {
    PersistentTable *table = tables.getGroupOffsetTable();
    TableIndex *index = table->primaryKeyIndex();

    TableTuple searchKey(index->getKeySchema());
    char data[searchKey.tupleLength()];
    searchKey.move(data);
    searchKey.setAllNulls();

    searchKey.setNValue(static_cast<int32_t>(GroupOffsetTable::IndexColumn::GROUP_ID), groupId);

    IndexCursor cursor(table->schema());
    index->moveToKeyOrGreater(&searchKey, cursor);

    TableTuple next;
    while (!(next = index->nextValue(cursor)).isNullTuple()
            && next.getNValue(static_cast<int32_t>(GroupOffsetTable::Column::GROUP_ID)) == groupId) {
        visitor(next);
    }
}

void GroupOffset::visitAll(const GroupTables& tables, const NValue& groupId,
        std::function<void(const GroupOffset&)> visitor) {
    visitAll(tables, groupId, [&tables, &groupId, &visitor](TableTuple& tuple) {
        GroupOffset offset(tables, tuple, groupId);
        visitor(offset);
    });
}

void GroupOffset::deleteIf(const GroupTables& tables, const NValue& groupId,
        std::function<bool(const GroupOffset&)> predicate) {
    std::vector<GroupOffset*> toDelete;

    visitAll(tables, groupId, [&tables, &groupId, &predicate, &toDelete](TableTuple& tuple) {
        GroupOffset *offset = new GroupOffset(tables, tuple, groupId);
        if (predicate(*offset)) {
            toDelete.push_back(offset);
        } else {
            delete offset;
        }
    });

    for (auto offset : toDelete) {
        offset->markForDelete();
        offset->commit(0L);
        delete offset;
    }
}

GroupOffset::GroupOffset(const GroupTables& tables, const NValue& groupId, const NValue& topic, int32_t partition) :
    GroupOrmBase(tables, groupId), m_topic(topic), m_partition(partition) {
    TableIndex *index = getTable()->primaryKeyIndex();

    TableTuple searchKey(index->getKeySchema());
    char data[searchKey.tupleLength()];
    searchKey.move(data);

    searchKey.setNValue(static_cast<int32_t>(GroupOffsetTable::IndexColumn::GROUP_ID), groupId);
    searchKey.setNValue(static_cast<int32_t>(GroupOffsetTable::IndexColumn::TOPIC), topic);
    searchKey.setNValue(static_cast<int32_t>(GroupOffsetTable::IndexColumn::PARTITION),
            ValueFactory::getIntegerValue(partition));

    IndexCursor cursor(getTable()->schema());
    index->moveToKey(&searchKey, cursor);
    setTableTuple(cursor.m_match);
}

GroupOffset::GroupOffset(const GroupTables& tables, TableTuple& tuple, const NValue& groupId) :
        GroupOrmBase(tables, tuple, groupId), m_topic(getNValue(GroupOffsetTable::Column::TOPIC)),
        m_partition(ValuePeeker::peekInteger(getNValue(GroupOffsetTable::Column::PARTITION))) {}

void GroupOffset::update(const OffsetCommitRequestPartition& request) {
    vassert(request.partitionIndex() == getPartition());

    if (isDeleted()) {
        // Need to initialize all values and not just do an update
        std::vector<NValue> values({
            getGroupId(),
            getTopic(),
            ValueFactory::getIntegerValue(getPartition()),
            ValueFactory::getTimestampValue(-1),
            ValueFactory::getBigIntValue(request.offset()),
            ValueFactory::getIntegerValue(request.leaderEpoch()),
            request.metadata()
        });

        setNValues(values);
    } else {
        std::vector<NValue> values({
            ValueFactory::getBigIntValue(request.offset()),
            ValueFactory::getIntegerValue(request.leaderEpoch()),
            request.metadata()
        });

        setNValues(values, GroupOffsetTable::Column::COMMITTED_OFFSET);
    }
}

void GroupOffset::commit(int64_t timestamp) {
    if (isDirty() && !isDeleted()) {
        setNValue(GroupOffsetTable::Column::COMMIT_TIMESTAMP, ValueFactory::getTimestampValue(timestamp));
    }
    GroupOrmBase::commit(timestamp);
}

bool GroupOffset::equalDeleted(const GroupOrmBase &other) const {
    const GroupOffset &otherOffset = dynamic_cast<const GroupOffset&>(other);
    return getGroupId() == other.getGroupId() && getTopic() == otherOffset.getTopic()
            && getPartition() == otherOffset.getPartition();
}

} /* namespace topics */
} /* namespace voltdb */
