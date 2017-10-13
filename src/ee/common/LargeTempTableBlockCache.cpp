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

#include <sstream>

#include "LargeTempTableBlockCache.h"

#include "common/Topend.h"
#include "common/executorcontext.hpp"
#include "common/FixUnusedAssertHack.h"
#include "common/SQLException.h"

namespace voltdb {

LargeTempTableBlockCache::LargeTempTableBlockCache(int64_t maxCacheSizeInBytes)
    : m_maxCacheSizeInBytes(maxCacheSizeInBytes)
    , m_blockList()
    , m_idToBlockMap()
    , m_nextId(0)
    , m_totalAllocatedBytes(0)
{
}

LargeTempTableBlockCache::~LargeTempTableBlockCache() {
    assert (m_blockList.size() == 0);
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
    auto mapIt = m_idToBlockMap.find(blockId);
    if (mapIt == m_idToBlockMap.end()) {
        throwDynamicSQLException("Request for unknown block ID in LargeTempTableBlockCache");
    }

    auto listIt = mapIt->second;
    if (! (*listIt)->isResident()) {
        Topend* topend = ExecutorContext::getExecutorContext()->getTopend();
        bool rc = topend->loadLargeTempTableBlock((*listIt)->id(), listIt->get());
        assert(rc);
        assert ((*listIt)->isPinned());
    }
    else {
        (*listIt)->pin();
    }

    // Also need to move it to the front of the queue.
    std::unique_ptr<LargeTempTableBlock> blockPtr;
    blockPtr.swap(*listIt);

    m_blockList.erase(listIt);
    m_blockList.emplace_front(std::move(blockPtr));
    m_idToBlockMap[blockId] = m_blockList.begin();

    return m_blockList.begin()->get();
}

void LargeTempTableBlockCache::unpinBlock(int64_t blockId) {
    auto mapIt = m_idToBlockMap.find(blockId);
    if (mapIt == m_idToBlockMap.end()) {
        throwDynamicSQLException("Request for unknown block ID in LargeTempTableBlockCache");
    }

    (*(mapIt->second))->unpin();
}

bool LargeTempTableBlockCache::blockIsPinned(int64_t blockId) const {
    auto mapIt = m_idToBlockMap.find(blockId);
    if (mapIt == m_idToBlockMap.end()) {
        throwDynamicSQLException("Request for unknown block ID in LargeTempTableBlockCache");
    }

    return (*(mapIt->second))->isPinned();
}

void LargeTempTableBlockCache::releaseBlock(int64_t blockId) {
    auto mapIt = m_idToBlockMap.find(blockId);
    if (mapIt == m_idToBlockMap.end()) {
        throwDynamicSQLException("Request for unknown block ID in LargeTempTableBlockCache");
    }


    auto it = mapIt->second;
    if (! (*it)->isResident()) {
        Topend* topend = ExecutorContext::getExecutorContext()->getTopend();
        bool rc = topend->releaseLargeTempTableBlock(blockId);
        assert(rc);
    }

    m_idToBlockMap.erase(blockId);
    // Block list contains unique_ptrs so erasing will invoke
    // destructors and free resources.
    m_blockList.erase(it);
}

void LargeTempTableBlockCache::releaseAllBlocks() {
    if (! m_blockList.empty()) {
        m_idToBlockMap.clear();

        Topend* topend = ExecutorContext::getExecutorContext()->getTopend();
        BOOST_FOREACH (auto& block, m_blockList) {
            if (block->isPinned()) {
                block->unpin();
            }

            if (! block->isResident()) {
                bool rc = topend->releaseLargeTempTableBlock(block->id());
                assert(rc);
            }
        }
        m_blockList.clear();
    }
}

void LargeTempTableBlockCache::storeABlock() {

    auto it = m_blockList.end();
    do {
        --it;
        LargeTempTableBlock *block = it->get();
        if (!block->isPinned() && block->isResident()) {
            Topend* topend = ExecutorContext::getExecutorContext()->getTopend();
            bool success = topend->storeLargeTempTableBlock(block->id(), block);
            if (! success) {
                throwDynamicSQLException("Topend failed to store LTT block");
            }
            return;
        }
    }
    while (it != m_blockList.begin());

    throwDynamicSQLException("Failed to find unpinned LTT block to make space");
}

void LargeTempTableBlockCache::increaseAllocatedMemory(int64_t numBytes) {
    m_totalAllocatedBytes += numBytes;

    // If we've increased the memory footprint over the size of the
    // cache, clear out some space.
    while (m_totalAllocatedBytes > maxCacheSizeInBytes()) {
        int64_t bytesBefore = m_totalAllocatedBytes;
        storeABlock();
        assert(bytesBefore > m_totalAllocatedBytes);
    }
}

void LargeTempTableBlockCache::decreaseAllocatedMemory(int64_t numBytes) {
    assert(numBytes <= m_totalAllocatedBytes);
    m_totalAllocatedBytes -= numBytes;
}

std::string LargeTempTableBlockCache::debug() const {
    std::ostringstream oss;
    oss << "LargeTempTableBlockCache:\n";
    BOOST_FOREACH(auto& block, m_blockList) {
        bool isResident = block->isResident();
        oss << "  Block id " << block->id() << ": "
            << (block->isPinned() ? "" : "un") << "pinned, "
            << (isResident ? "" : "not ") << "resident\n";
        oss << "  Tuple count: " << block->activeTupleCount() << "\n";
        oss << "    Using " << block->getAllocatedMemory() << " bytes \n";
        oss << "      " << block->getAllocatedTupleMemory() << " bytes for tuple storage\n";
        oss << "      " << block->getAllocatedPoolMemory() << " bytes for pool storage\n";
    }

    oss << "Total bytes used: " << allocatedMemory() << "\n";

    return oss.str();
}

} // end namespace voltdb
