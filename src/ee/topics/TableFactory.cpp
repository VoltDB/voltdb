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

#include <vector>
#include <string>

#include "topics/TableFactory.h"

namespace voltdb { namespace topics {

const std::string GroupTable::name = "_topics_group";
const std::string GroupTable::pkIndexName = GroupTable::name + "_pkey";
const std::string GroupTable::standaloneGroupIndexName = GroupTable::name + "_standalone_index";

const std::string GroupMemberTable::name = "_topics_group_member";
const std::string GroupMemberTable::indexName = GroupMemberTable::name + "_index";

const std::string GroupOffsetTable::name = "_topics_group_offset";
const std::string GroupOffsetTable::indexName = GroupOffsetTable::name + "_pkey";

/*
 * Abstract expression implementation for testing if a group tuple represents a  standalone group
 */
class GroupTableStandalonePredicate : public AbstractExpression {
    NValue eval(const TableTuple* tuple, const TableTuple* unused = nullptr) const override {
        NValue protocol = tuple->getNValue(static_cast<int32_t>(GroupTable::Column::PROTOCOL));
        int32_t length;
        ValuePeeker::peekObject_withoutNull(protocol, &length);
        return length == 0 ? NValue::getTrue() : NValue::getFalse();
    }

    std::string debugInfo(const std::string &spacer) const override {
        return spacer + "GroupTableStandalonePredicate";
    }
};

/*
 * Create a table for tracking topics groups equivalent to
 * CREATE TABLE _topics_group (id VARCHAR(256 BYTES) NOT NULL, generation INTEGER NOT NULL,
 *     leader VARCHAR(36 BYTES) NOT NULL, protocol VARCHAR(256 BYTES) NOT NULL, PRIMARY KEY (id));
 * PARTITION TABLE topics_group ON COLUMN id;
 */
PersistentTable* TableFactory::createGroup(const SystemTableFactory &factory) {
    std::vector<std::string> columnNames = { "id", "update_timestamp", "generation", "leader", "protocol" };
    std::vector<ValueType> columnTypes = { ValueType::tVARCHAR, ValueType::tTIMESTAMP, ValueType::tINTEGER,
            ValueType::tVARCHAR, ValueType::tVARCHAR };
    std::vector<int32_t> columnSizes = { 256, 0, 0, 36, 256 };
    std::vector<bool> allowNull = { false, false, false, true, false };
    std::vector<bool> columnInBytes = { true, false, false, true, true };

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, columnInBytes);
    std::vector<int32_t> pkeyColumns = { static_cast<int32_t>(GroupTable::Column::ID) };

    PersistentTable* table = factory.createTable(GroupTable::name, schema, columnNames, 0);
    factory.addIndex(table, GroupTable::pkIndexName, pkeyColumns);
    factory.addIndex(table, GroupTable::standaloneGroupIndexName, pkeyColumns, true, false,
            new GroupTableStandalonePredicate());

    return table;
}

/*
 * Create a table for tracking topics group members equivalent to
 * CREATE TABLE _topics_group_member (group_id VARCHAR(256 BYTES) NOT NULL, id VARCHAR(36 BYTES) NOT NULL,
 *     client_id VARCHAR(256 BYTES) NOT NULL, client_host VARCHAR(256 BYTES) NOT NULL,
 *     session_timeout INTEGER NOT NULL, rebalance_timeout INTEGER NOT NULL, instance_id VARCHAR(256 BYTES),
 *     protocol_metadata VARBINARY(1048576) NOT NULL, assignments VARBINARY(1048576) NOT NULL);
 * PARTITION TABLE topics_group_member ON COLUMN group_id;
 */
PersistentTable* TableFactory::createGroupMember(const SystemTableFactory &factory) {
    std::vector<std::string> columnNames = { "group_id", "id", "client_id", "client_host", "session_timeout",
            "rebalance_timeout", "instance_id",
            "protocol_metadata", "assignments" };
    std::vector<ValueType> columnTypes = { ValueType::tVARCHAR, ValueType::tVARCHAR,
            ValueType::tVARCHAR, ValueType::tVARCHAR, ValueType::tINTEGER,
            ValueType::tINTEGER, ValueType::tVARCHAR, ValueType::tVARBINARY, ValueType::tVARBINARY };
    std::vector<int32_t> columnSizes = { 256, 36, 256, 256, 0, 0, 256, 1048576, 1048576 };
    std::vector<bool> allowNull = { false, false, false, false, false, false, true, false, false };
    std::vector<bool> columnInBytes = { true, true, true, true, false, false, true, true, true };

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, columnInBytes);
    std::vector<int32_t> indexColumns = { static_cast<int32_t>(GroupMemberTable::Column::GROUP_ID) };

    PersistentTable* table = factory.createTable(GroupMemberTable::name, schema, columnNames, 0);
    factory.addIndex(table, GroupMemberTable::indexName, indexColumns, false);
    return table;
}

/*
 * Create a table for tracking persisted offsets for a topics group equivalent to
 * CREATE TABLE _topics_group_offset (group_id VARCHAR(256 BYTES) NOT NULL, topic VARCHAR(256 BYTES) NOT NULL,
 *     partition INTEGER NOT NULL, committed_offset BIGINT NOT NULL, expires TIMESTAMP NOT NULL,
 *     leader_epoch INTEGER NOT NULL, metadata VARCHAR(1048576), PRIMARY KEY (group_id, topic, partition));
 * PARTITION TABLE topics_group_offset ON COLUMN group_id;
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
