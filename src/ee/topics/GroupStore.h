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

#include <stdint.h>

#include "storage/persistenttable.h"
#include "topics/GroupTables.h"

namespace voltdb { namespace topics {

/**
 * High level API for interacting with the topics system tables. This API allows group and group offsets to be stored,
 * retrieved and deleted
 */
class GroupStore : public GroupTables {
public:
    virtual ~GroupStore();

    /**
     * Initialize this instance from the provided engine
     */
    void initialize(VoltDBEngine* engine);

    /**
     * Initialize this instance from the explicitly specified values
     */
    void initialize(PersistentTable* group, PersistentTable* groupMember, PersistentTable* groupOffset);

    /**
     * Perform an upsert to update the metadata about a topics group and its members
     */
    void storeGroup(SerializeInputBE& groupMetadata);

    /**
     * Delete the group, all members and offsets which have the given groupId
     */
    void deleteGroup(const NValue& groupId);

    /**
     * Fetch topics groups in a serialized format from the topics system tables. A fetch is initialized whenever
     * maxSize is > 0. If maxSize <= 0 then this method will continue fetching groups until all groups have been
     * fetched.
     *
     * @param maxResultSize maximum size of data to serialize in out
     * @param startGroupId Non inclusive groupId at which to start fetching
     * @param out buffer where the groups being returned are to be serialized
     * @return true if there are more groups to return otherwise false
     */
    bool fetchGroups(int maxResultSize, const NValue& startGroupId, SerializeOutput& out);

    /**
     * Store offsets and associate them with the provided group. Offsets are serialized in the topics wire format
     *
     * @param spUniqueId unique ID for this transaction
     * @param requestVersion Version of the commit request message
     * @param groupId where the offsets should be stored
     * @param offsets serialized set offsets for topics and partitions
     * @param out buffer where the response to the commit will be serialized
     */
    void commitOffsets(int64_t spUniqueId, int16_t requestVersion, const NValue& groupId, SerializeInputBE& offsets,
            SerializeOutput& out);

    /**
     * Fetch offsets for the given group and serialize them to out. If any topics and partitions are specified only
     * those offsets will be returned. Topic partitions are serialized in the topics wire format
     *
     * @param requestVersion Version of the fetch request message
     * @param groupId from which to fetch offsets
     * @param topicPartitions serialized set of topics and partitions to fetch. If empty all are fetched
     * @param out buffer where the response to the fetch will be serialized
     */
    void fetchOffsets(int16_t requestVersion, const NValue& groupId, SerializeInputBE& topicPartitions,
            SerializeOutput& out);

    /**
     * Delete the offsets of standalone groups which are older than the given timestamp
     * @param deleteOlderThan any offsets older than this will be deleted
     */
    void deleteExpiredOffsets(const int64_t deleteOlderThan);

    // Satisfy the GroupTables interface
    PersistentTable* getGroupTable() const override { return m_group; }
    PersistentTable* getGroupMemberTable() const override { return m_groupMember; }
    PersistentTable* getGroupOffsetTable() const override { return m_groupOffset; }

private:
    PersistentTable* m_group = nullptr;
    PersistentTable* m_groupMember = nullptr;
    PersistentTable* m_groupOffset = nullptr;
};

} }
