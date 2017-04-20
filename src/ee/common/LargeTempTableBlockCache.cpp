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

namespace voltdb {

    LargeTempTableBlockCache::LargeTempTableBlockCache()
        : m_cache()
          //, m_emptyEntries(NUM_CACHE_ENTRIES)
        , m_liveEntries()
        // , m_unpinnedEntries()
    {
        // At initialization, all cache entries are empty.
        // auto cacheIt = m_cache.begin();
        // for (; cacheIt != m_cache.end(); ++cacheIt) {
        //     m_emptyEntries.push_back(&(*cacheIt));
        // }
    }

    std::pair<int64_t, LargeTempTableBlock*> LargeTempTableBlockCache::getEmptyBlock(LargeTempTable* ltt) {
        int64_t id = getNextId();

        m_cache.emplace_back(ltt);
        LargeTempTableBlock *emptyBlock = &(m_cache.back());
        m_liveEntries[id] = emptyBlock;
        return std::make_pair(id, emptyBlock);
    }

    LargeTempTableBlock* LargeTempTableBlockCache::fetchBlock(int64_t blockId) {
        assert(m_liveEntries.find(blockId) != std::end(m_liveEntries));

        // return *(m_liveEntries.find(blockId));

        // // If it's in the unpinned entries list, remove it
        // auto unpinnedIt = m_unpinnedEntries.begin();
        // for (; unpinnedIt != m_unpinnedEntries.end(); ++unpinnedIt) {
        //     if (*unpinnedIt == blockId) {
        //         m_unpinnedEntries.erase(unpinnedIt);
        //         break;
        //     }
        // }

        return m_liveEntries[blockId];
    }

    void LargeTempTableBlockCache::unpinBlock(int64_t blockId) {
        //m_unpinnedEntries.push_front(blockId);
    }

    void LargeTempTableBlockCache::releaseBlock(int64_t blockId) {
        //assert(false);
    }
}
