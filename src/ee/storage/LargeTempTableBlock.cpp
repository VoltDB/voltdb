/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
#include "common/TupleSchema.h"

#include "storage/LargeTempTableBlock.h"
#include "storage/LargeTempTable.h"

namespace voltdb {

LargeTempTableBlock::LargeTempTableBlock(int64_t id, TupleSchema* schema)
    : m_id(id)
    , m_schema(schema)
    , m_storage(new char [BLOCK_SIZE_IN_BYTES])
    , m_tupleInsertionPoint(m_storage.get())
    , m_nonInlinedInsertionPoint(m_storage.get() + BLOCK_SIZE_IN_BYTES)
    , m_isPinned(false)
    , m_isStored(false)
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
    target.resetHeader();
    target.copyForPersistentInsert(source, this);
    target.setActiveTrue();

    // References to the interior of large temp table blocks are
    // volatile because the block could be swapped to disk.
    target.setInlinedDataIsVolatileTrue();
    target.setNonInlinedDataIsVolatileTrue();

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

void LargeTempTableBlock::setData(char* origAddress,
                                  std::unique_ptr<char[]> storage) {
    assert(m_storage.get() == NULL);
    storage.swap(m_storage);

    // Need to update all the string ref pointers in the tuples...
    char* storageAddr = m_storage.get();
    std::ptrdiff_t oldNewOffset = storageAddr - origAddress;

    TableTuple tuple{m_schema};
    int tupleLength = tuple.tupleLength();
    for (int i = 0; i < m_activeTupleCount; ++i) {
        char* tupleStorage = storageAddr + (tupleLength * i);
        tuple.move(tupleStorage);
        tuple.relocateNonInlinedFields(oldNewOffset);
    }
}

std::unique_ptr<char[]> LargeTempTableBlock::releaseData() {
    std::unique_ptr<char[]> storage;
    storage.swap(m_storage);
    m_isStored = true;
    return storage;
}

std::string LargeTempTableBlock::debug() const {
    std::ostringstream oss;
    oss << "Block " << m_id << ", " << m_activeTupleCount << " tuples, ";

    if (! isResident()) {
        oss << "not resident";
    }
    else {
        TableTuple tuple{m_storage.get(), m_schema};
        oss << "first tuple: " << tuple.debugSkipNonInlineData();
    }

    return oss.str();
}

std::string LargeTempTableBlock::debugUnsafe() const {
    std::ostringstream oss;
    oss << "Block " << m_id << ", " << m_activeTupleCount << " tuples, ";

    if (! isResident()) {
        oss << "not resident";
    }
    else {
        TableTuple tuple{m_storage.get(), m_schema};
        oss << "first tuple: " << tuple.debug();
    }

    return oss.str();
}

} // end namespace voltdb
