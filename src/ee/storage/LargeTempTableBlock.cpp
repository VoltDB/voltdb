/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

LargeTempTableBlock::LargeTempTableBlock(LargeTempTableBlockId id, const TupleSchema* schema) :
    m_id(id), m_schema(schema), m_tupleInsertionPoint(tupleStorage()),
    m_nonInlinedInsertionPoint(m_storage.get() + BLOCK_SIZE_IN_BYTES) {
    // Initialize the metadata in the header of the block:
    *getStorageAddressPointer() = &m_storage[0];
    *getStorageTupleCount() = 0;
}

bool LargeTempTableBlock::insertTuple(const TableTuple& source) {
    vassert(m_tupleInsertionPoint <= m_nonInlinedInsertionPoint);

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
    ++*getStorageTupleCount();
    vassert(m_activeTupleCount == *getStorageTupleCount());
    m_tupleInsertionPoint += target.tupleLength();

    // Make sure that the values we computed for the size check match
    // the actual sizes... m_nonInlinedInsertionPoint will have been
    // updated by a call to LargeTempTableBlock::allocate().
    vassert(m_tupleInsertionPoint == newTupleInsertionPoint);
    vassert(m_nonInlinedInsertionPoint == newNonInlinedInsertionPoint);

    return true;
}

void* LargeTempTableBlock::allocate(std::size_t size) {
    m_nonInlinedInsertionPoint -= size;
    vassert(m_tupleInsertionPoint <= m_nonInlinedInsertionPoint);
    return m_nonInlinedInsertionPoint;
}

int64_t LargeTempTableBlock::getAllocatedMemory() const {
    if (! isResident()) {
        return 0;
    } else {
        vassert(getAllocatedTupleMemory() + getAllocatedPoolMemory() <= BLOCK_SIZE_IN_BYTES);
        return BLOCK_SIZE_IN_BYTES;
    }
}

int64_t LargeTempTableBlock::getAllocatedTupleMemory() const {
    if (isResident()) {
        return m_tupleInsertionPoint - m_storage.get();
    } else {
        return 0;
    }
}

int64_t LargeTempTableBlock::getAllocatedPoolMemory() const {
    if (isResident()) {
        return m_storage.get() + BLOCK_SIZE_IN_BYTES - m_nonInlinedInsertionPoint;
    } else {
        return 0;
    }
}

void LargeTempTableBlock::setData(std::unique_ptr<char[]> storage) {
    vassert(m_storage.get() == nullptr);
    storage.swap(m_storage);

    vassert(m_activeTupleCount == *getStorageTupleCount());

    char* origAddress = *(getStorageAddressPointer());

    // Update the insertion points to reflect the relocation
    std::ptrdiff_t oldNewOffset = m_storage.get() - origAddress;
    m_tupleInsertionPoint += oldNewOffset;
    m_nonInlinedInsertionPoint += oldNewOffset;

    // Need to update all the string ref pointers in the tuples...
    for(auto& tuple : *this) {
        tuple.toTableTuple(m_schema).relocateNonInlinedFields(oldNewOffset);
    }

    *getStorageAddressPointer() = m_storage.get();
}

std::unique_ptr<char[]> LargeTempTableBlock::releaseData() {
    vassert(*getStorageAddressPointer() == m_storage.get());
    std::unique_ptr<char[]> storage;
    storage.swap(m_storage);
    m_isStored = true;
    return storage;
}

std::string LargeTempTableBlock::debug() const {
    std::ostringstream oss;
    oss << "Block " << m_id << ", " << m_activeTupleCount << " tuples  ";

    if (! isResident()) {
        oss << "(not resident)";
    } else {
        oss << "\n";
        TableTuple tuple{m_storage.get(), m_schema};
        if (m_activeTupleCount >= 1) {
            oss << "Block --> first tuple: " << tuple.debugSkipNonInlineData() << "\n";
        }

        if (m_activeTupleCount >= 2) {
            char *lastTupleAddress = m_storage.get() + (tuple.tupleLength() * (m_activeTupleCount - 1));
            tuple.move(lastTupleAddress);
            oss << "Block --> last tuple: " << tuple.debugSkipNonInlineData() << "\n";
        }
    }

    return oss.str();
}

std::string LargeTempTableBlock::debugUnsafe() const {
    std::ostringstream oss;
    oss << "Block " << m_id << ", " << m_activeTupleCount << " tuples, ";

    if (! isResident()) {
        oss << "not resident";
    } else {
        TableTuple tuple{m_storage.get(), m_schema};
        oss << "first tuple: " << tuple.debug();
    }

    return oss.str();
}

LargeTempTableBlock::iterator LargeTempTableBlock::begin() {
    return iterator(m_schema, tupleStorage());
}

LargeTempTableBlock::const_iterator LargeTempTableBlock::begin() const {
    return const_iterator(m_schema, tupleStorage());
}

LargeTempTableBlock::const_iterator LargeTempTableBlock::cbegin() const {
    return const_iterator(m_schema, tupleStorage());
}

LargeTempTableBlock::iterator LargeTempTableBlock::end() {
    TableTuple tuple(tupleStorage(), m_schema);
    char *endAddress = tupleStorage() + (tuple.tupleLength() * m_activeTupleCount);
    return iterator(m_schema, endAddress);
}

LargeTempTableBlock::const_iterator LargeTempTableBlock::end() const {
    TableTuple tuple(tupleStorage(), m_schema);
    char *endAddress = tupleStorage() + (tuple.tupleLength() * m_activeTupleCount);
    return const_iterator(m_schema, endAddress);
}

LargeTempTableBlock::const_iterator LargeTempTableBlock::cend() const {
    TableTuple tuple(tupleStorage(), m_schema);
    char *endAddress = m_storage.get() + (tuple.tupleLength() * m_activeTupleCount);
    return const_iterator(m_schema, endAddress);
}

} // end namespace voltdb
