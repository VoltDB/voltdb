/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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

    LargeTempTableBlock::LargeTempTableBlock(LargeTempTable *ltt)
        : m_pool(new Pool())
        , m_tupleBlockPointer(new TupleBlock(ltt, TBBucketPtr()))

    {
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

    int64_t LargeTempTableBlock::getAllocatedMemory() const {

        return
            (m_pool.get() ? m_pool->getAllocatedMemory() : 0)
            + (m_tupleBlockPointer.get() ? m_tupleBlockPointer->getAllocatedMemory() : 0);
    }

    LargeTempTableBlock::~LargeTempTableBlock() {
        LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
        lttBlockCache->decreaseAllocatedMemory(getAllocatedMemory());
    }

    std::unique_ptr<Pool> LargeTempTableBlock::releasePool() {
        LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
        lttBlockCache->decreaseAllocatedMemory(m_pool->getAllocatedMemory());

        std::unique_ptr<Pool> poolPtr;
        poolPtr.swap(m_pool);

        return poolPtr;
    }

    TBPtr LargeTempTableBlock::releaseBlock() {
        LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
        lttBlockCache->decreaseAllocatedMemory(m_tupleBlockPointer->getAllocatedMemory());

        TBPtr blockPtr;
        blockPtr.swap(m_tupleBlockPointer);

        return blockPtr;
    }
}
