/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

#include "storage/LargeTempTableBlock.h"
#include "storage/LargeTempTable.h"

namespace voltdb {

LargeTempTableBlock::LargeTempTableBlock(int64_t id)
    : m_id(id)
    , m_storage(new char [BLOCK_SIZE_IN_BYTES])
    , m_tupleInsertionPoint(m_storage.get())
    , m_nonInlinedInsertionPoint(m_storage.get() + BLOCK_SIZE_IN_BYTES)
    , m_isPinned(false)
    , m_activeTupleCount(0)
{
}

bool LargeTempTableBlock::insertTuple(const TableTuple& source) {
    assert (m_tupleInsertionPoint <= m_nonInlinedInsertionPoint);

    size_t nonInlinedMemorySize = source.getNonInlinedMemorySizeForTempTable();
    int tupleLength = source.tupleLength();

    char* newTupleInsertionPoint = m_tupleInsertionPoint + tupleLength;
    char* newNonInlinedInsertionPoint = m_nonInlinedInsertionPoint - nonInlinedMemorySize;

    if (newTupleInsertionPoint > newNonInlinedInsertionPoint) {
        // Not enough room in this block for this tuple and its
        // non-inlined values.
        return false;
    }

    TableTuple target(source.getSchema());
    target.move(m_tupleInsertionPoint);
    target.copyForPersistentInsert(source, this);
    target.setActiveTrue();

    ++m_activeTupleCount;
    m_tupleInsertionPoint += target.tupleLength();

    // Make sure that the values we computed for the size check match
    // the actual sizes... m_nonInlinedInsertionPoint will have been
    // updated by a call to LargeTempTableBlock::allocate().
    assert(m_tupleInsertionPoint == newTupleInsertionPoint);
    assert(m_nonInlinedInsertionPoint == newNonInlinedInsertionPoint);

    return true;
}

void* LargeTempTableBlock::allocate(std::size_t size) {
    m_nonInlinedInsertionPoint -= size;
    assert(m_tupleInsertionPoint <= m_nonInlinedInsertionPoint);
    return m_nonInlinedInsertionPoint;
}

int64_t LargeTempTableBlock::getAllocatedMemory() const {
    if (! isResident()) {
        return 0;
    }

    assert (getAllocatedTupleMemory() + getAllocatedPoolMemory() <= BLOCK_SIZE_IN_BYTES);
    return BLOCK_SIZE_IN_BYTES;
}

int64_t LargeTempTableBlock::getAllocatedTupleMemory() const {
    if (isResident()) {
        return m_tupleInsertionPoint - m_storage.get();
    }

    return 0;
}

int64_t LargeTempTableBlock::getAllocatedPoolMemory() const {
    if (isResident()) {
        return (m_storage.get() + BLOCK_SIZE_IN_BYTES) - m_nonInlinedInsertionPoint;
    }

    return 0;
}

void LargeTempTableBlock::setData(std::unique_ptr<char[]> storage) {
    assert(m_storage.get() == NULL);
    storage.swap(m_storage);
}

std::unique_ptr<char[]> LargeTempTableBlock::releaseData() {
    std::unique_ptr<char[]> storage;
    storage.swap(m_storage);
    return storage;
}

} // end namespace voltdb
