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

#include "topics/messages/OffsetCommit.h"
#include "topics/orm/GroupOrmBase.h"

namespace voltdb {
namespace topics {

class GroupOffset: public GroupOrmBase {
public:
    /*
     * Visit all offsets for a groupId. Each offset will be passed to the visitor
     */
    static void visitAll(const GroupTables& tables, const NValue& groupId,
            std::function<void(const GroupOffset&)> visitor);

    /*
     * Delete offsets for a groupId which match the given predicate
     */
    static void deleteIf(const GroupTables &tables, const NValue &groupId,
            std::function<bool(const GroupOffset&)> predicate);

    GroupOffset(const GroupTables& tables, const NValue& groupId, const NValue& topic, int32_t partition);

    /**
     * @return the topic which this offset is for. Type VARCHAR
     */
    const NValue& getTopic() const {
        return m_topic;
    }

    /**
     * @return the partition which this offset is for
     */
    const int32_t getPartition() const {
        return m_partition;
    }

    /**
     * @return the stored offset
     */
    const int64_t getOffset() const {
        return ValuePeeker::peekBigInt(getNValue(GroupOffsetTable::Column::COMMITTED_OFFSET));
    }

    /**
     * @return the partition leader epoch for the offset as provided by the client
     */
    const int32_t getLeaderEpoch() const {
        return ValuePeeker::peekInteger(getNValue(GroupOffsetTable::Column::LEADER_EPOCH));
    }

    /**
     * @return the client provieded metadata associated with this offset
     */
    const NValue getMetadata() const {
        return getNValue(GroupOffsetTable::Column::METADATA);
    }

    /**
     * Returns the timestamp for when this groffsetoup was last committed or -1 if this offset was never committed
     */
    const int64_t getCommitTimestamp() const {
        return isInTable() ? ValuePeeker::peekTimestamp(getNValue(GroupOffsetTable::Column::COMMIT_TIMESTAMP)) : -1;
    }

    /**
     * Update this offset with the information from request
     */
    void update(const OffsetCommitRequestPartition& request);

    void commit(int64_t timestamp) override;

protected:
    bool equalDeleted(const GroupOrmBase& other) const override;

    PersistentTable* getTable() const override { return m_tables.getGroupOffsetTable(); }

private:
    static void visitAll(const GroupTables& tables, const NValue& groupId, std::function<void(TableTuple&)> visitor);

    GroupOffset(const GroupTables& tables, TableTuple& tuple, const NValue& groupId);

    // Name of topic
    const NValue m_topic;
    // Partition ID
    const int32_t m_partition;
};

} /* namespace topics */
} /* namespace voltdb */
