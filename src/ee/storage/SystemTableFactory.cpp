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

#include "SystemTableFactory.h"

#include "common/TupleSchema.h"
#include "indexes/tableindexfactory.h"
#include "storage/tablefactory.h"

using namespace voltdb;

PersistentTable* SystemTableFactory::createTable(char const *name, TupleSchema *schema,
        const std::vector<std::string> &columnNames, const int partitionColumn,
        const std::vector<int32_t> &primaryKeyColumns) {
    vassert(partitionColumn >= 0 && partitionColumn < columnNames.size());

    Table *table = TableFactory::getPersistentTable(0, name, schema, columnNames, nullptr, false, partitionColumn,
            TableType::PERSISTENT, 0, INT_MAX, m_compactionThreshold, false, false);
    PersistentTable *persistentTable = dynamic_cast<PersistentTable*>(table);

    if (primaryKeyColumns.size()) {
        std::string indexName = std::string(name) + "_pkey";
        auto indexedExpressions = TableIndex::simplyIndexColumns();
        TableIndexScheme scheme(indexName, BALANCED_TREE_INDEX, primaryKeyColumns, indexedExpressions, true, false,
                false, table->schema());

        TableIndex *index = TableIndexFactory::getInstance(scheme);
        persistentTable->addIndex(index);
        persistentTable->setPrimaryKeyIndex(index);
    }

    return persistentTable;
}

/*
 * Create a table for tracking kipling groups equivalent to
 * CREATE TABLE _kipling_group (id VARCHAR(256 BYTES) NOT NULL, generation INTEGER NOT NULL, state TINYINT NOT NULL,
 *     leader BIGINT NOT NULL, protocol VARCHAR(256 BYTES), PRIMARY KEY (id));
 * PARTITION TABLE kipling_group ON COLUMN id;
 */
PersistentTable* SystemTableFactory::createKiplingGroup() {
    std::vector<std::string> columnNames = { "id", "generation", "state", "leader", "protocol" };
    std::vector<ValueType> columnTypes = { ValueType::tVARCHAR, ValueType::tINTEGER, ValueType::tTINYINT,
            ValueType::tBIGINT, ValueType::tVARCHAR };
    std::vector<int32_t> columnSizes = { 256, 0, 0, 0, 256 };
    std::vector<bool> allowNull = { false, false, false, false, true };
    std::vector<bool> columnInBytes = { true, false, false, false, true };

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, columnInBytes);

    return createTable("_kipling_group", schema, columnNames, 0, { 0 });
}

/*
 * Create a table for tracking kipling group members equivalent to
 * CREATE TABLE _kipling_group_member (group_id VARCHAR(256 BYTES) NOT NULL, id BIGINT NOT NULL,
 *     instance_id VARCHAR(256 BYTES), assignments VARBINARY(1048576), PRIMARY KEY (group_id, id));
 * PARTITION TABLE kipling_group_member ON COLUMN group_id;
 */
PersistentTable* SystemTableFactory::createKiplingGroupMember() {
    std::vector<std::string> columnNames = { "group_id", "id", "instance_id", "assignments" };
    std::vector<ValueType> columnTypes = { ValueType::tVARCHAR, ValueType::tBIGINT, ValueType::tVARCHAR,
            ValueType::tVARBINARY };
    std::vector<int32_t> columnSizes = { 256, 0, 256, 1048576 };
    std::vector<bool> allowNull = { false, false, true, true };
    std::vector<bool> columnInBytes = { true, false, true, true };

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, columnInBytes);

    return createTable("_kipling_group_member", schema, columnNames, 0, { 0, 1 });
}

/*
 * Create a table for tracking kipling registered protocols per member per group equivalent to
 * CREATE TABLE _kipling_group_member_protocol (group_id VARCHAR(256 BYTES) NOT NULL, id BIGINT NOT NULL,
 *     protocol VARCHAR(256 BYTES) NOT NULL, metadata VARBINARY(1048576) NOT NULL, PRIMARY KEY (group_id, id, protocol));
 * PARTITION TABLE kipling_group_member_protocol ON COLUMN group_id;
 */
PersistentTable* SystemTableFactory::createKiplingGroupMemberProtocol() {
    std::vector<std::string> columnNames = { "group_id", "id", "protocol", "metadata" };
    std::vector<ValueType> columnTypes = { ValueType::tVARCHAR, ValueType::tBIGINT, ValueType::tVARCHAR,
            ValueType::tVARBINARY };
    std::vector<int32_t> columnSizes = { 256, 0, 256, 1048576 };
    std::vector<bool> allowNull = { false, false, false, false };
    std::vector<bool> columnInBytes = { true, false, true, true };

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, columnInBytes);

    return createTable("_kipling_group_member_protocol", schema, columnNames, 0, { 0, 1, 2 });
}

/*
 * Create a table for tracking persisted offsets for a kipling group equivalent to
 * CREATE TABLE _kipling_group_offset (group_id VARCHAR(256 BYTES) NOT NULL, topic VARCHAR(256 BYTES) NOT NULL,
 *     partition INTEGER NOT NULL, committed_offset BIGINT NOT NULL, leader_epoch INTEGER NOT NULL,
 *     metadata VARCHAR(1048576), PRIMARY KEY (group_id, topic, partition));
 * PARTITION TABLE kipling_group_offset ON COLUMN group_id;
 */
PersistentTable* SystemTableFactory::createKiplingGroupOffset() {
    std::vector<std::string> columnNames = { "group_id", "topic", "partition", "committed_offset", "leader_epoch",
            "metadata" };
    std::vector<ValueType> columnTypes = { ValueType::tVARCHAR, ValueType::tVARCHAR, ValueType::tINTEGER,
            ValueType::tBIGINT, ValueType::tINTEGER, ValueType::tVARCHAR };
    std::vector<int32_t> columnSizes = { 256, 256, 0, 0, 0, 1048576 };
    std::vector<bool> allowNull = { false, false, false, false, false, true };
    std::vector<bool> columnInBytes = { true, true, false, false, false, true };

    TupleSchema *schema = TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, columnInBytes);

    return createTable("_kipling_group_offset", schema, columnNames, 0, { 0, 1, 2 });
}

PersistentTable* SystemTableFactory::create(const SystemTableId id) {
    switch (id) {
    case SystemTableId::KIPLING_GROUP:
        return createKiplingGroup();
    case SystemTableId::KIPLING_GROUP_MEMBER:
        return createKiplingGroupMember();
    case SystemTableId::KIPLING_GROUP_MEMBER_PROTOCOL:
        return createKiplingGroupMemberProtocol();
    case SystemTableId::KIPLING_GROUP_OFFSET:
        return createKiplingGroupOffset();
    default:
        std::string errorMessage = "Unknown system table ID: " + std::to_string(static_cast<int32_t>(id));
        throw SerializableEEException(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_GENERIC, errorMessage);
    }
}
