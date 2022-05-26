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
#include <topics/orm/GroupOrmBase.h>
#include "indexes/tableindex.h"

namespace voltdb { namespace topics {

void GroupOrmBase::commit(int64_t timestamp) {
    if (!isDirty()) {
        return;
    }

    // Determine if the change is an insert, update or delete
    if (!isInTable()) {
        // Not in table must be an insert
        m_tableTuple = getTable()->insertPersistentTuple(m_update, true);
    } else if (m_update.isNullTuple()) {
        // No update data must be a delete
        getTable()->deleteTuple(m_tableTuple);
        m_tableTuple.move(nullptr);
    } else {
        // Perform standard update
        getTable()->updateTupleWithSpecificIndexes(m_tableTuple, m_update, m_updatedIndexes);
    }

    // Update performed so clear state and prepare for other potential updates
    m_updatedIndexes.clear();
    freeUpdateData();
    m_dirty = false;
}

void GroupOrmBase::markForDelete() {
    freeUpdateData();
    m_dirty = isInTable();
}

bool GroupOrmBase::operator==(const GroupOrmBase &other) {
    if (getTable() != other.getTable() || isDirty() != other.isDirty() || isInTable() != other.isInTable()
            || isDeleted() != other.isDeleted()) {
        return false;
    }

    // If both deleted
    if (isDeleted()) {
        return equalDeleted(other);
    }

    int columnCount = getTable()->schema()->columnCount();
    for (int i = 0; i < columnCount; ++i) {
        if (getNValue(i) != other.getNValue(i)) {
            return false;
        }
    }

    return true;
}

void GroupOrmBase::setTableTuple(const TableTuple& tableTuple) {
    m_tableTuple = tableTuple;
    setSchema(tableTuple.getSchema());
}

void GroupOrmBase::setNValues(const std::vector<NValue>& values, int startIndex) {
    vassert(startIndex >= 0);
    vassert(values.size() + startIndex <= m_update.getSchema()->columnCount());
    setupForUpdate();

    int index = startIndex;
    for (const NValue& value : values) {
        m_update.setNValue(index++, value);
    }
}

void GroupOrmBase::setupForUpdate() {
    if (m_update.isNullTuple()) {
        m_update.moveAndInitialize(malloc(m_update.tupleLength()));

        if (isInTable()) {
            m_update.copy(m_tableTuple);
        }
    }

    m_dirty = true;
}

void GroupOrmBase::freeUpdateData() {
    char* address = m_update.address();
    if (address != nullptr) {
        m_update.move(nullptr);
        free(address);
    }
}

} }

