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

#include "LargeTempTableBlockCache.h"

#include "common/Topend.h"
#include "common/executorcontext.hpp"

namespace voltdb {

    LargeTempTableBlockCache::LargeTempTableBlockCache()
        : m_blockList()
        , m_idToBlockMap()
        , m_nextId(0)
        , m_totalAllocatedBytes(0)
    {
    }

    std::pair<int64_t, LargeTempTableBlock*> LargeTempTableBlockCache::getEmptyBlock(LargeTempTable* ltt) {
        int64_t id = getNextId();

        m_blockList.emplace_front(new LargeTempTableBlock(id, ltt));
        auto it = m_blockList.begin();
        m_idToBlockMap[id] = it;
        (*it)->pin();

        return std::make_pair(id, m_blockList.front().get());
    }

    LargeTempTableBlock* LargeTempTableBlockCache::fetchBlock(int64_t blockId) {
        LargeTempTableBlock *block = m_idToBlockMap[blockId]->get();
        if (! block->isResident()) {
            Topend* topend = ExecutorContext::getExecutorContext()->getTopend();
            bool rc = topend->loadLargeTempTableBlock(block->id(), block);
            assert(rc);
        }
        block->pin();

        // Also need to move it to the front of the queue.

        return block;
    }

    void LargeTempTableBlockCache::unpinBlock(int64_t blockId) {
        (*m_idToBlockMap[blockId])->unpin();
    }

    void LargeTempTableBlockCache::releaseBlock(int64_t blockId) {
        auto it = m_idToBlockMap[blockId];
        if (! (*it)->isResident()) {
            Topend* topend = ExecutorContext::getExecutorContext()->getTopend();
            bool rc = topend->releaseLargeTempTableBlock(blockId);
            assert(rc);
        }

        m_idToBlockMap.erase(blockId);
        m_blockList.erase(it);
    }

    bool LargeTempTableBlockCache::storeABlock() {
        // Start at the end of the list

        auto it = m_blockList.end();
        do {
            --it;
            LargeTempTableBlock *block = it->get();
            if (!block->isPinned() && block->isResident()) {
                Topend* topend = ExecutorContext::getExecutorContext()->getTopend();
                return topend->storeLargeTempTableBlock(block->id(), block);
            }

        }
        while (it != m_blockList.begin());

        return false;
    }

    void LargeTempTableBlockCache::increaseAllocatedMemory(int64_t numBytes) {
        m_totalAllocatedBytes += numBytes;

        if (m_totalAllocatedBytes > CACHE_SIZE_IN_BYTES()) {
            // Okay, we've increased the memory footprint over the size of the
            // cache.  Clear out some space.
            while (m_totalAllocatedBytes > CACHE_SIZE_IN_BYTES()) {
                int64_t bytesBefore = m_totalAllocatedBytes;
                if (!storeABlock()) {
                    throw std::logic_error("could not store a block to make space");
                }

                assert(bytesBefore > m_totalAllocatedBytes);
            }
        }

    }

    void LargeTempTableBlockCache::decreaseAllocatedMemory(int64_t numBytes) {
        assert(numBytes <= m_totalAllocatedBytes);
        m_totalAllocatedBytes -= numBytes;
    }
}
