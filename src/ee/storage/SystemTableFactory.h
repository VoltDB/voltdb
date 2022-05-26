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

#include "storage/persistenttable.h"
#include "storage/SystemTableId.h"

namespace voltdb {

class SystemTableFactory {

public:
    SystemTableFactory(int32_t compactionThreshold = 95): m_compactionThreshold(compactionThreshold) {}

    /**
     * Return a vector containing all SystemTableIds
     */
    static inline const std::vector<SystemTableId> getAllSystemTableIds() {
        return { SystemTableId::TOPICS_GROUP, SystemTableId::TOPICS_GROUP_MEMBER,
            SystemTableId::TOPICS_GROUP_OFFSET };
    }

    /**
     * Create a new PersistentTable for the system table with id.
     */
    PersistentTable* create(const SystemTableId id);

    // Common utility method for creating a system table
    PersistentTable *createTable(const std::string& name, TupleSchema *schema, const std::vector<std::string> &columnNames,
            const int partitionColumn) const {
        return createTable(name.c_str(), schema, columnNames, partitionColumn);
    }
    PersistentTable *createTable(char const *name, TupleSchema *schema, const std::vector<std::string> &columnNames,
            const int partitionColumn) const;

    // Add an index to the table. unique must be true for the index to be a primary key
    void addIndex(PersistentTable *table, const std::string suffix, const std::vector<int32_t> &columns,
            bool unique = true, bool primary = true, AbstractExpression* predicate = nullptr) const;
private:
    // Member variables
    int32_t m_compactionThreshold;
};
}
