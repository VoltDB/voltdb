/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/PersistentTableTupleFilter.h"

namespace voltdb {
void PersistentTableTupleFilter::init(Table* table) {
    vassert(table != nullptr);
    m_tuples.insert(m_tuples.end(), table->activeTupleCount(), INACTIVE_TUPLE);
    TableTuple tuple(table->schema());
    m_lastActiveTupleIndex = table->activeTupleCount() - 1;
    TableIterator iterator = table->iterator();
    uint64_t tupleIdx = 0;
    while (iterator.next(tuple)) {
        m_tuples[tupleIdx] = ACTIVE_TUPLE;
        uint64_t tupleAddress = reinterpret_cast<uint64_t>(tuple.address());
        m_tupleIndexes.insert({tupleAddress, tupleIdx});
        tupleIdx++;
    }
#ifdef VOLT_TRACE_ENABLED
    for (auto it = m_tupleIndexes.begin(); it != m_tupleIndexes.end(); ++it) {
       std::cout << it->left << " with index " << it->right << std::endl;
    }
#endif
}

uint64_t PersistentTableTupleFilter::getTupleIndex(const TableTuple& tuple) {
    uint64_t tupleAddress = reinterpret_cast<uint64_t>(tuple.address());
    uint64_t idx = m_tupleIndexes.left.find(tupleAddress)->second;
    return idx;
}

uint64_t PersistentTableTupleFilter::getTupleAddress(size_t tupleIdx) {
    uint64_t address = m_tupleIndexes.right.find(tupleIdx)->second;
    return address;
}
}
