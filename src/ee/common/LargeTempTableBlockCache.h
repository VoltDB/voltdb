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

#ifndef VOLTDB_LARGETEMPTABLEBLOCKCACHE_H
#define VOLTDB_LARGETEMPTABLEBLOCKCACHE_H

#include <deque>
#include <list>
#include <map>
#include <utility>
#include <vector>

#include <boost/scoped_array.hpp>

#include "storage/LargeTempTableBlock.h"
#include "common/types.h"

class LargeTempTableTest_OverflowCache;

namespace voltdb {

    class LargeTempTable;

    // xxx This class really belongs in storage
    class LargeTempTableBlockCache {

        friend class ::LargeTempTableTest_OverflowCache;

    public:
        LargeTempTableBlockCache();

        std::pair<int64_t, LargeTempTableBlock*> getEmptyBlock(LargeTempTable* ltt);

        void unpinBlock(int64_t blockId);

        LargeTempTableBlock* fetchBlock(int64_t blockId);

        void releaseBlock(int64_t blockId);

        void increaseAllocatedMemory(int64_t numBytes);
        void decreaseAllocatedMemory(int64_t numBytes);

        size_t numPinnedEntries() const {
            size_t cnt = 0;
            BOOST_FOREACH(auto &block, m_blockList) {
                if (block->isPinned()) {
                    ++cnt;
                }
            }

            return cnt;
        }

        size_t residentBlockCount() const {
            size_t count = 0;
            BOOST_FOREACH(auto &block, m_blockList) {
                if (block->isResident()) {
                    ++count;
                }
            }

            return count;
        }

        size_t totalBlockCount() const {
            return m_blockList.size();
        }

        int64_t allocatedMemory() const {
            return m_totalAllocatedBytes;
        }

    private:

        // Set to be modifiable here for testing purposes
        static int64_t& CACHE_SIZE_IN_BYTES() {
            static int64_t cacheSizeInBytes = 50 * 1024 * 1024; // 50 MB
            return cacheSizeInBytes;
        }

        int64_t getNextId() {
            int64_t nextId = m_nextId;
            ++m_nextId;
            return nextId;
        }

        bool storeABlock();

        typedef std::list<std::unique_ptr<LargeTempTableBlock>> BlockList;

        // The front of the block list are the most recently used blocks.
        // The tail will be the least recently used blocks.
        // The tail of the list should have no pinned blocks.
        BlockList m_blockList;
        std::map<int64_t, BlockList::iterator> m_idToBlockMap;

        int64_t m_nextId;
        int64_t m_totalAllocatedBytes;
    };
}

#endif // VOLTDB_LARGETEMPTABLEBLOCKCACHE_H
