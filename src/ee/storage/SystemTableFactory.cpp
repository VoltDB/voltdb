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

#include "common/TupleSchema.h"
#include "indexes/tableindexfactory.h"
#include "kipling/TableFactory.h"
#include "storage/tablefactory.h"
#include "storage/SystemTableFactory.h"

using namespace voltdb;

PersistentTable* SystemTableFactory::createTable(char const *name, TupleSchema *schema,
        const std::vector<std::string> &columnNames, const int partitionColumn) const {
    vassert(partitionColumn >= 0 && partitionColumn < columnNames.size());

    Table *table = TableFactory::getPersistentTable(0, name, schema, columnNames, nullptr, false, partitionColumn,
            TableType::PERSISTENT, 0, INT_MAX, m_compactionThreshold, false, false);
    return dynamic_cast<PersistentTable*>(table);
}

void SystemTableFactory::addIndex(PersistentTable *table, const std::string name, const std::vector<int32_t> &columns,
        bool unique, bool primary) const {
    auto indexedExpressions = TableIndex::simplyIndexColumns();
    TableIndexScheme scheme(name, BALANCED_TREE_INDEX, columns, indexedExpressions, unique, false, false,
            table->schema());

    TableIndex *index = TableIndexFactory::getInstance(scheme);
    table->addIndex(index);
    if (unique && primary) {
        table->setPrimaryKeyIndex(index);
    }
}

PersistentTable* SystemTableFactory::create(const SystemTableId id) {
    switch (id) {
    case SystemTableId::KIPLING_GROUP:
        return kipling::TableFactory::createGroup(*this);
    case SystemTableId::KIPLING_GROUP_MEMBER:
        return kipling::TableFactory::createGroupMember(*this);
    case SystemTableId::KIPLING_GROUP_MEMBER_PROTOCOL:
        return kipling::TableFactory::createGroupMemberProtocol(*this);
    case SystemTableId::KIPLING_GROUP_OFFSET:
        return kipling::TableFactory::createGroupOffset(*this);
    default:
        std::string errorMessage = "Unknown system table ID: " + std::to_string(static_cast<int32_t>(id));
        throw SerializableEEException(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_GENERIC, errorMessage);
    }
}
