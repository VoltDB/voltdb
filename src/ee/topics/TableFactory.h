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

#pragma once

#include <storage/SystemTableFactory.h>

namespace voltdb { namespace topics {

// Static classes to describe the different system tables
struct GroupTable {
    static const std::string name;
    static const std::string pkIndexName;
    static const std::string standaloneGroupIndexName;

    enum class Column : int8_t {
        ID = 0, COMMIT_TIMESTAMP, GENERATION, LEADER, PROTOCOL
    };

    enum class IndexColumn : int8_t {
        ID = 0
    };
};

struct GroupMemberTable {
    static const std::string name;
    static const std::string indexName;

    enum class Column : int8_t {
        GROUP_ID = 0, MEMBER_ID, CLIENT_ID, CLIENT_HOST, SESSION_TIMEOUT, REBALANCE_TIMEOUT, INSTANCE_ID, PROTOCOL_METADATA, ASSIGNMENTS
    };

    enum class IndexColumn : int8_t {
        GROUP_ID = 0
    };
};

struct GroupOffsetTable {
    static const std::string name;
    static const std::string indexName;

    enum class Column : int8_t {
        GROUP_ID = 0, TOPIC, PARTITION, COMMIT_TIMESTAMP, COMMITTED_OFFSET, LEADER_EPOCH, METADATA
    };

    enum class IndexColumn : int8_t {
        GROUP_ID = 0, TOPIC, PARTITION
    };
};

class TableFactory {

public:
    /* Create a new PersistentTable for topics groups */
    static PersistentTable* createGroup(const SystemTableFactory &factory);
    /* Create a new PersistentTable for topics group members */
    static PersistentTable* createGroupMember(const SystemTableFactory &factory);
    /* Create a new PersistentTable for topics group offsets */
    static PersistentTable* createGroupOffset(const SystemTableFactory &factory);
};

} }
