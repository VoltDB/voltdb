/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "storage/TupleBlock.h"
#include "storage/table.h"

namespace voltdb {
TupleBlock::TupleBlock(Table *table) :
        m_references(0),
        m_storage(new char[table->m_tableAllocationSize]),
        m_tupleLength(table->m_tupleLength),
        m_tuplesPerBlock(table->m_tuplesPerBlock),
        m_activeTuples(0),
        m_nextFreeTuple(0) {
}

double TupleBlock::loadFactor() {
    return m_activeTuples / static_cast<double>(m_tuplesPerBlock);
}

bool TupleBlock::hasFreeTuples() {
    return m_activeTuples < m_tuplesPerBlock;
}

bool TupleBlock::isEmpty() {
    if (m_activeTuples == 0) {
        return true;
    }
    return false;
}

char* TupleBlock::nextFreeTuple() {
    char *retval = NULL;
    if (!m_freeList.empty()) {
        retval = m_storage.get();
        TruncatedInt offset = m_freeList.back();
        m_freeList.pop_back();

        if (m_freeList.capacity() / 2 > m_freeList.size()) {
            std::vector<TruncatedInt>(m_freeList).swap(m_freeList);
        }
        retval += offset.unpack();
    } else {
        retval = &(m_storage.get()[m_tupleLength * m_nextFreeTuple]);
        m_nextFreeTuple++;
    }
    m_activeTuples++;
    return retval;
}

void TupleBlock::freeTuple(char *tupleStorage) {
    m_activeTuples--;
    //Find the offset
    uint32_t offset = static_cast<uint32_t>(tupleStorage - m_storage.get());
    m_freeList.push_back(offset);
}

char * TupleBlock::address() {
    return m_storage.get();
}

void TupleBlock::reset() {
    m_activeTuples = 0;
    m_nextFreeTuple = 0;
    m_freeList.clear();
}

uint32_t TupleBlock::unusedTupleBoundry() {
    return m_nextFreeTuple;
}
}

