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

LargeTempTableBlock::LargeTempTableBlock(int64_t id, LargeTempTable *ltt)
    : m_id(id)
    , m_pool(new Pool(ltt->getTableAllocationSize() / 4, 1))
    , m_tupleBlockPointer(new TupleBlock(ltt, TBBucketPtr()))
    , m_storage(new char [BLOCK_SIZE_IN_BYTES])
    , m_tupleInsertionPoint(m_storage)
    , m_stringInsertionPoint(m_storage + BLOCK_SIZE_IN_BYTES)
    , m_isPinned(false)
{
    // Report the amount of memory used by this block.
    //
    // Even though it has zero tuples, this is the memory for the
    // tuple block and the first chunk in the string pool.
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    lttBlockCache->increaseAllocatedMemory(getAllocatedMemory());
}

bool LargeTempTableBlock::hasFreeTuples() const {
    return m_tupleBlockPointer->hasFreeTuples();
}

void LargeTempTableBlock::insertTuple(const TableTuple& source) {
    TableTuple target(source.getSchema());
    int64_t origPoolMemory = m_pool->getAllocatedMemory();

    char* data;
    std::tie(data, std::ignore) = m_tupleBlockPointer->nextFreeTuple();
    target.move(data);
    target.copyForPersistentInsert(source, m_pool.get());
    target.setActiveTrue();

    int64_t increasedMemory = m_pool->getAllocatedMemory() - origPoolMemory;
    if (increasedMemory > 0) {
        LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
        lttBlockCache->increaseAllocatedMemory(increasedMemory);
    }
}

bool LargeTempTableBlock::insertTupleNew(const TableTuple& source) {
    assert (m_tupleInsertionPoint < m_stringInsertionPoint);

    // TODO: Need to account for StringRef instances??
    size_t stringMemorySize = source.getNonInlinedMemorySize();
    int tupleLength = source.tupleLength();

    if ((m_tupleInsertionPoint + tupleLength) >
        (m_stringInsertionPoint - stringMemorySize)) {

        // Not enough room in this block for this tuple and its
        // outlined values.
        return false;
    }

    TableTuple target(source.getSchema());
    target.move(m_tupleInsertionPoint);
    target.copyForPersistentInsert(source, this);
    target.setActiveTrue();

    return true;
}

void* LargeTempTableBlock::allocate(std::size_t size) {
    m_stringInsertionPoint -= size;
    assert(m_tupleInsertionPoint < m_stringInsertionPoint);
    return m_stringInsertionPoint;
}

int64_t LargeTempTableBlock::getAllocatedMemory() const {
    return getAllocatedTupleMemory() + getAllocatedPoolMemory();
}

int64_t LargeTempTableBlock::getAllocatedTupleMemory() const {
    if (isResident())
        return m_tupleBlockPointer->getAllocatedMemory();

    return 0;
}

int64_t LargeTempTableBlock::getAllocatedPoolMemory() const {
    if (isResident())
        return m_pool->getAllocatedMemory();

    return 0;
}


LargeTempTableBlock::~LargeTempTableBlock() {
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    lttBlockCache->decreaseAllocatedMemory(getAllocatedMemory());

    delete [] m_storage;
}

std::pair<TBPtr, std::unique_ptr<Pool>> LargeTempTableBlock::releaseData() {
    TBPtr tbptr;
    std::unique_ptr<Pool> pool;

    tbptr.swap(m_tupleBlockPointer);
    pool.swap(m_pool);

    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    lttBlockCache->decreaseAllocatedMemory(tbptr->getAllocatedMemory());
    lttBlockCache->decreaseAllocatedMemory(pool->getAllocatedMemory());

    return std::make_pair(tbptr, std::move(pool));
}

void LargeTempTableBlock::setData(TBPtr tbptr, std::unique_ptr<Pool> pool) {
    assert(pool.get() != NULL && m_pool.get() == NULL);
    assert(tbptr.get() != NULL && m_tupleBlockPointer.get() == NULL);
    tbptr.swap(m_tupleBlockPointer);
    pool.swap(m_pool);

    // pin this block so we don't try to expel it when reporting memory to cache
    pin();

    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    lttBlockCache->increaseAllocatedMemory(m_tupleBlockPointer->getAllocatedMemory());
    lttBlockCache->increaseAllocatedMemory(m_pool->getAllocatedMemory());
}

} // end namespace voltdb
