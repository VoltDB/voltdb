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
#include "kipling/orm/GroupOffset.h"

namespace voltdb {
namespace kipling {

void GroupOffset::visitAll(const GroupTables& tables, const NValue& groupId, std::function<void(GroupOffset&)> visitor) {
    PersistentTable* table = tables.getGroupOffsetTable();
    TableIndex *index = table->primaryKeyIndex();

    TableTuple searchKey(index->getKeySchema());
    char data[searchKey.tupleLength()];
    searchKey.move(data);
    searchKey.setAllNulls();

    searchKey.setNValue(static_cast<int32_t>(GroupOffsetTable::IndexColumn::GROUP_ID), groupId);

    IndexCursor cursor(table->schema());
    index->moveToKeyOrGreater(&searchKey, cursor);

    TableTuple next;
    while (!(next = index->nextValue(cursor)).isNullTuple()) {
        if (next.getNValue(static_cast<int32_t>(GroupOffsetTable::Column::GROUP_ID)) != groupId) {
            break;
        }

        GroupOffset offset(tables, next, groupId);
        visitor(offset);
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

} /* namespace kipling */
} /* namespace voltdb */
