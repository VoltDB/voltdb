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
        : m_cache()
        , m_liveEntries()
        , m_pinnedEntries()
        , m_storedEntries()
        , m_totalAllocatedBytes(0)
    {
    }

    std::pair<int64_t, LargeTempTableBlock*> LargeTempTableBlockCache::getEmptyBlock(LargeTempTable* ltt) {
        int64_t id = getNextId();

        m_cache.emplace_back(new LargeTempTableBlock(ltt));
        LargeTempTableBlock *emptyBlock = m_cache.back().get();
        m_liveEntries[id] = emptyBlock;
        m_pinnedEntries.insert(id);

        return std::make_pair(id, emptyBlock);
    }

    bool LargeTempTableBlockCache::loadBlock(int64_t blockId) {
        Topend* topend = ExecutorContext::getExecutorContext()->getTopend();
        LargeTempTableBlock* block = topend->loadLargeTempTableBlock(blockId);

        if (block == NULL) {
            return false;
        }

        m_cache.emplace_back(block);
        m_liveEntries[blockId] = block;

        // callee will pin the block.

        return true;
    }

    LargeTempTableBlock* LargeTempTableBlockCache::fetchBlock(int64_t blockId) {
        if (m_liveEntries.find(blockId) == std::end(m_liveEntries)) {
            bool rc = loadBlock(blockId);
            assert(rc);
        }

        assert(m_liveEntries.find(blockId) != std::end(m_liveEntries));
        assert(m_pinnedEntries.find(blockId) == m_pinnedEntries.end());
        m_pinnedEntries.insert(blockId);

        return m_liveEntries[blockId];
    }

    void LargeTempTableBlockCache::unpinBlock(int64_t blockId) {
        assert(m_pinnedEntries.find(blockId) != m_pinnedEntries.end());
        m_pinnedEntries.erase(blockId);
    }

    void LargeTempTableBlockCache::releaseBlock(int64_t blockId) {
        assert(m_pinnedEntries.find(blockId) == m_pinnedEntries.end());
        assert(m_liveEntries.find(blockId) != m_liveEntries.end());

        auto liveIt = m_liveEntries.find(blockId);
        LargeTempTableBlock* block = liveIt->second;
        m_liveEntries.erase(blockId);

        auto cacheIt = m_cache.begin();
        for (; cacheIt != m_cache.end(); ++cacheIt) {
            if (cacheIt->get() == block) {
                m_cache.erase(cacheIt);
                break;
            }
        }
    }

    bool LargeTempTableBlockCache::storeABlock() {
        // Just store the first unpinned block that we can find.
        auto liveIt = m_liveEntries.begin();
        for (; liveIt != m_liveEntries.end(); ++liveIt) {
            if (m_pinnedEntries.find(liveIt->first) == m_pinnedEntries.end()) {
                Topend* topend = ExecutorContext::getExecutorContext()->getTopend();
                int64_t blockId = liveIt->first;
                LargeTempTableBlock *block = liveIt->second;

                m_storedEntries[blockId] = block->getAllocatedMemory();
                topend->storeLargeTempTableBlock(blockId, block);

                m_liveEntries.erase(blockId);
                auto cacheIt = m_cache.begin();
                for (; cacheIt != m_cache.end(); ++cacheIt) {
                    if (cacheIt->get() == block) {
                        m_cache.erase(cacheIt);
                        break;
                    }
                }

                return true;
            }
        }

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
