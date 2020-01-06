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

#include <vector>
#include <string>

#include "kipling/TableFactory.h"

namespace voltdb { namespace kipling {

const std::string GroupTable::name = "_kipling_group";
const std::string GroupTable::indexName = GroupTable::name + "_pkey";

const std::string GroupMemberTable::name = "_kipling_group_member";
const std::string GroupMemberTable::indexName = GroupMemberTable::name + "_index";

const std::string GroupMemberProtocolTable::name = "_kipling_group_member_protocol";
const std::string GroupMemberProtocolTable::indexName = GroupMemberProtocolTable::name + "_pkey";

const std::string GroupOffsetTable::name = "_kipling_group_offset";
const std::string GroupOffsetTable::indexName = GroupOffsetTable::name + "_pkey";

/*
 * Create a table for tracking kipling groups equivalent to
 * CREATE TABLE _kipling_group (id VARCHAR(256 BYTES) NOT NULL, generation INTEGER NOT NULL, state TINYINT NOT NULL,
 *     leader VARCHAR(36 BYTES) NOT NULL, protocol VARCHAR(256 BYTES), PRIMARY KEY (id));
 * PARTITION TABLE kipling_group ON COLUMN id;
 */
PersistentTable* TableFactory::createGroup(const SystemTableFactory &factory) {
    std::vector<std::string> columnNames = { "id", "update_timestamp", "generation", "state", "leader", "protocol" };
    std::vector<ValueType> columnTypes = { ValueType::tVARCHAR, ValueType::tTIMESTAMP, ValueType::tINTEGER,
            ValueType::tTINYINT, ValueType::tVARCHAR, ValueType::tVARCHAR };
    std::vector<int32_t> columnSizes = { 256, 0, 0, 0, 36, 256 };
    std::vector<bool> allowNull = { false, false, false, false, true, true };
    std::vector<bool> columnInBytes = { true, false, false, false, true, true };

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, columnInBytes);
    std::vector<int32_t> pkeyColumns = { static_cast<int32_t>(GroupTable::Column::ID) };

    PersistentTable* table = factory.createTable(GroupTable::name, schema, columnNames, 0);
    factory.addIndex(table, GroupTable::indexName, pkeyColumns);
    return table;
}

/*
 * Create a table for tracking kipling group members equivalent to
 * CREATE TABLE _kipling_group_member (group_id VARCHAR(256 BYTES) NOT NULL, id VARCHAR(36 BYTES) NOT NULL,
 *     session_timeout BIGINT NOT NULL, instance_id VARCHAR(256 BYTES), assignments VARBINARY(1048576),
 *     PRIMARY KEY (group_id, id));
 * PARTITION TABLE kipling_group_member ON COLUMN group_id;
 */
PersistentTable* TableFactory::createGroupMember(const SystemTableFactory &factory) {
    std::vector<std::string> columnNames = { "group_id", "id", "session_timeout", "rebalance_timeout", "instance_id",
            "assignments", "flags" };
    std::vector<ValueType> columnTypes = { ValueType::tVARCHAR, ValueType::tVARCHAR, ValueType::tINTEGER,
            ValueType::tINTEGER, ValueType::tVARCHAR, ValueType::tVARBINARY, ValueType::tSMALLINT };
    std::vector<int32_t> columnSizes = { 256, 36, 0, 0, 256, 1048576, 0 };
    std::vector<bool> allowNull = { false, false, false, false, true, true, false };
    std::vector<bool> columnInBytes = { true, true, false, false, true, true, false };

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, columnInBytes);
    std::vector<int32_t> indexColumns = { static_cast<int32_t>(GroupMemberTable::Column::GROUP_ID) };

    PersistentTable* table = factory.createTable(GroupMemberTable::name, schema, columnNames, 0);
    factory.addIndex(table, GroupMemberTable::indexName, indexColumns, false);
    return table;
}

/*
 * Create a table for tracking kipling registered protocols per member per group equivalent to
 * CREATE TABLE _kipling_group_member_protocol (group_id VARCHAR(256 BYTES) NOT NULL, id VARCHAR(36 BYTES) NOT NULL,
 *     protocol VARCHAR(256 BYTES) NOT NULL, metadata VARBINARY(1048576) NOT NULL, PRIMARY KEY (group_id, id, protocol));
 * PARTITION TABLE kipling_group_member_protocol ON COLUMN group_id;
 */
PersistentTable* TableFactory::createGroupMemberProtocol(const SystemTableFactory &factory) {
    std::vector<std::string> columnNames = { "group_id", "id", "index", "protocol", "metadata" };
    std::vector<ValueType> columnTypes = { ValueType::tVARCHAR, ValueType::tVARCHAR, ValueType::tSMALLINT,
            ValueType::tVARCHAR, ValueType::tVARBINARY };
    std::vector<int32_t> columnSizes = { 256, 36, 0, 256, 1048576 };
    std::vector<bool> allowNull = { false, false, false, false, false };
    std::vector<bool> columnInBytes = { true, true, false, true, true };

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, columnInBytes);
    std::vector<int32_t> indexColumns = { static_cast<int32_t>(GroupMemberProtocolTable::Column::GROUP_ID),
            static_cast<int32_t>(GroupMemberProtocolTable::Column::MEMBER_ID) };

    PersistentTable* table = factory.createTable(GroupMemberProtocolTable::name, schema, columnNames, 0);
    factory.addIndex(table, GroupMemberProtocolTable::indexName, indexColumns, false);
    return table;
}

/*
 * Create a table for tracking persisted offsets for a kipling group equivalent to
 * CREATE TABLE _kipling_group_offset (group_id VARCHAR(256 BYTES) NOT NULL, topic VARCHAR(256 BYTES) NOT NULL,
 *     partition INTEGER NOT NULL, committed_offset BIGINT NOT NULL, expires TIMESTAMP NOT NULL,
 *     leader_epoch INTEGER NOT NULL, metadata VARCHAR(1048576), PRIMARY KEY (group_id, topic, partition));
 * PARTITION TABLE kipling_group_offset ON COLUMN group_id;
 */
PersistentTable* TableFactory::createGroupOffset(const SystemTableFactory &factory) {
    std::vector<std::string> columnNames = { "group_id", "topic", "partition", "commit_timestamp", "committed_offset",
            "leader_epoch", "metadata" };
    std::vector<ValueType> columnTypes = { ValueType::tVARCHAR, ValueType::tVARCHAR, ValueType::tINTEGER,
            ValueType::tTIMESTAMP, ValueType::tBIGINT, ValueType::tINTEGER, ValueType::tVARCHAR };
    std::vector<int32_t> columnSizes = { 256, 256, 0, 0, 0, 0, 32768 };
    std::vector<bool> allowNull = { false, false, false, false, false, false, true};
    std::vector<bool> columnInBytes = { true, true, false, false, false, false, true };

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, columnInBytes);
    std::vector<int32_t> pkeyColumns = { static_cast<int32_t>(GroupOffsetTable::Column::GROUP_ID),
            static_cast<int32_t>(GroupOffsetTable::Column::TOPIC),
            static_cast<int32_t>(GroupOffsetTable::Column::PARTITION) };

    PersistentTable* table = factory.createTable(GroupOffsetTable::name, schema, columnNames, 0);
    factory.addIndex(table, GroupOffsetTable::indexName, pkeyColumns);
    return table;
}

} }
