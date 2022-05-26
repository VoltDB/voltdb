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

#include <sstream>

#include "LargeTempTableBlockCache.h"

#include "common/Topend.h"
#include "common/executorcontext.hpp"

namespace voltdb {

LargeTempTableBlockCache::LargeTempTableBlockCache(Topend *topend,
                                                   int64_t maxCacheSizeInBytes,
                                                   LargeTempTableBlockId::siteId_t siteId)
    : m_topend(topend)
    , m_maxCacheSizeInBytes(maxCacheSizeInBytes)
    , m_blockList()
    , m_idToBlockMap()
    , m_nextId(siteId, 0)
    , m_totalAllocatedBytes(0) { }

LargeTempTableBlockCache::~LargeTempTableBlockCache() {
    vassert(m_blockList.size() == 0);
}

LargeTempTableBlock* LargeTempTableBlockCache::getEmptyBlock(const TupleSchema* schema) {
    ensureSpaceForNewBlock();

    LargeTempTableBlockId id = getNextId();

    m_blockList.emplace_back(new LargeTempTableBlock(id, schema));
    auto it = m_blockList.end();
    --it;
    m_idToBlockMap[id] = it;
    (*it)->pin();

    m_totalAllocatedBytes += LargeTempTableBlock::BLOCK_SIZE_IN_BYTES;

    return it->get();
}

LargeTempTableBlock* LargeTempTableBlockCache::fetchBlock(LargeTempTableBlockId blockId) {
    auto mapIt = m_idToBlockMap.find(blockId);
    if (mapIt == m_idToBlockMap.end()) {
        throwSerializableEEException("Request for unknown block ID in LargeTempTableBlockCache (fetch)");
    }

    auto listIt = mapIt->second;
    vassert((*listIt)->id() == blockId);
    if (! (*listIt)->isResident()) {
        ++m_numCacheMisses;
        ensureSpaceForNewBlock();
        __attribute__((unused)) bool rc =
                m_topend->loadLargeTempTableBlock(listIt->get());
        vassert(rc);
        vassert(! (*listIt)->isPinned());
        m_totalAllocatedBytes += LargeTempTableBlock::BLOCK_SIZE_IN_BYTES;
    }
    else {
        ++m_numCacheHits;
    }

    (*listIt)->pin();

    // Also need to move it to the back of the queue.
    std::unique_ptr<LargeTempTableBlock> blockPtr;
    blockPtr.swap(*listIt);

    m_blockList.erase(listIt);
    m_blockList.emplace_back(std::move(blockPtr));
    auto it = m_blockList.end();
    --it;
    m_idToBlockMap[blockId] = it;

    LargeTempTableBlock* block = it->get();
    vassert(block->id() == blockId);
    return block;
}

void LargeTempTableBlockCache::unpinBlock(LargeTempTableBlockId blockId) {
    auto mapIt = m_idToBlockMap.find(blockId);
    if (mapIt == m_idToBlockMap.end()) {
        throwSerializableEEException("Request for unknown block ID in LargeTempTableBlockCache (unpin)");
    }

    (*(mapIt->second))->unpin();
}

bool LargeTempTableBlockCache::blockIsPinned(LargeTempTableBlockId blockId) const {
    auto mapIt = m_idToBlockMap.find(blockId);
    if (mapIt == m_idToBlockMap.end()) {
        throwSerializableEEException("Request for unknown block ID in LargeTempTableBlockCache (blockIsPinned)");
    }

    return (*(mapIt->second))->isPinned();
}

void LargeTempTableBlockCache::invalidateStoredCopy(LargeTempTableBlock* block) {
    if (! block->isStored()) {
        return;
    }

    bool success = m_topend->releaseLargeTempTableBlock(block->id());
    if (! success) {
        throwSerializableEEException("Release of large temp table block failed");
    }

    block->unstore();
}


void LargeTempTableBlockCache::releaseBlock(LargeTempTableBlockId blockId) {
    auto mapIt = m_idToBlockMap.find(blockId);
    if (mapIt == m_idToBlockMap.end()) {
        throwSerializableEEException("Request for unknown block ID in LargeTempTableBlockCache (release)");
    }

    auto it = mapIt->second;
    if ((*it)->isPinned()) {
        throwSerializableEEException("Request to release pinned block");
    }

    if ((*it)->isStored()) {
        bool success = m_topend->releaseLargeTempTableBlock(blockId);
        if (! success) {
            throwSerializableEEException("Release of large temp table block failed");
        }
    }

    if ((*it)->isResident()) {
        m_totalAllocatedBytes -= LargeTempTableBlock::BLOCK_SIZE_IN_BYTES;
        vassert(m_totalAllocatedBytes >= 0);
    }

    m_idToBlockMap.erase(blockId);
    // Block list contains unique_ptrs so erasing will invoke
    // destructors and free resources.
    m_blockList.erase(it);
}

void LargeTempTableBlockCache::releaseAllBlocks() {
    if (! m_blockList.empty()) {
        BOOST_FOREACH (auto& block, m_blockList) {
            if (block->isPinned()) {
                throwSerializableEEException("Request to release pinned block (releaseAllBlocks)");
            }
            if (block->isStored()) {
                __attribute__((unused)) bool rc = m_topend->releaseLargeTempTableBlock(block->id());
                vassert(rc);
            }

            if (block->isResident()) {
                m_totalAllocatedBytes -= LargeTempTableBlock::BLOCK_SIZE_IN_BYTES;
                vassert(m_totalAllocatedBytes >= 0);
            }

            m_idToBlockMap.erase(block->id());
        }
        m_blockList.clear();
    }

    vassert(m_totalAllocatedBytes == 0);
    vassert(m_blockList.empty());
    vassert(m_idToBlockMap.empty());
}

void LargeTempTableBlockCache::ensureSpaceForNewBlock() {
    if (m_totalAllocatedBytes + LargeTempTableBlock::BLOCK_SIZE_IN_BYTES <= m_maxCacheSizeInBytes) {
        return; // There is already enough space
    }

    if (m_blockList.empty()) {
        vassert(m_totalAllocatedBytes == 0);
        throwSerializableEEException("LTT block cache needs a block be stored but there are no blocks");
    }

    auto it = m_blockList.end();
    do {
        --it;
        LargeTempTableBlock *block = it->get();
        vassert(block != NULL);
        if (!block->isPinned() && block->isResident()) {
            // this block may have already been stored, in which case
            // we do not need to store it again.
            if (! block->isStored()) {
                bool success = m_topend->storeLargeTempTableBlock(block);
                if (! success) {
                    throwSerializableEEException("Topend failed to store LTT block");
                }
            }
            else {
                // Block is already stored, so just release its storage.
                block->releaseData();
            }

            m_totalAllocatedBytes -= LargeTempTableBlock::BLOCK_SIZE_IN_BYTES;
            vassert(m_totalAllocatedBytes >= 0);
            vassert(! block->isResident());
            return;
        }
    }
    while (it != m_blockList.begin());

    throwSerializableEEException("Failed to find unpinned LTT block to make space");
}

std::string LargeTempTableBlockCache::debug() const {
    std::ostringstream oss;
    oss << "LargeTempTableBlockCache:\n";
    BOOST_FOREACH(auto& block, m_blockList) {
        if (block.get() != NULL) {
            bool isResident = block->isResident();
            oss << "  Block id " << block->id() << ": "
                << (block->isPinned() ? "" : "un") << "pinned, "
                << (isResident ? "" : "not ") << "resident, "
                << (block->isStored() ? "" : "not ") << "stored\n";
            oss << "  Tuple count: " << block->activeTupleCount() << "\n";
            oss << "    Using " << block->getAllocatedMemory() << " bytes \n";
            oss << "      " << block->getAllocatedTupleMemory() << " bytes for tuple storage\n";
            oss << "      " << block->getAllocatedPoolMemory() << " bytes for pool storage\n";
        }
        else {
            oss << "  Mysteriously NULL block pointer\n";
        }
    }

    oss << "Total bytes used: " << allocatedMemory() << "\n";

    return oss.str();
}

std::string LargeTempTableBlockCache::statsForDebug() const {
    std::ostringstream oss;
    oss << "LargeTempTableBlockCache stats:\n"
        << "    Number of cache hits:    " << m_numCacheHits << "\n"
        << "    Number of cache misses:  " << m_numCacheMisses << "\n";
    return oss.str();
}

} // end namespace voltdb
