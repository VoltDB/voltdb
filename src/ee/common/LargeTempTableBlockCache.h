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

#include <array>
#include <deque>
#include <list>
#include <map>
#include <utility>
#include <vector>

#include <boost/scoped_array.hpp>

#include "common/types.h"

namespace voltdb {

    // Move this to ee/storage? xxx
    class LargeTempTableBlock {
        friend class LargeTempTableTest_MultiBlock;
    public:
        LargeTempTableBlock()
            : m_data(new char[getBlocksize()])
            , m_usedBytes(0)
        {
        }

        static size_t getBlocksize() {
            return blocksizeRef();
        }

        char* getData() {
            return m_data;
        }

        void incrementUsedBytes(size_t byteCount) {
            m_usedBytes += byteCount;
        }

        size_t getUsedBytes() const {
            return m_usedBytes;
        }

        size_t getRemainingBytes() const {
            return getBlocksize() - getUsedBytes();
        }

        void makeEmpty() {
            m_usedBytes = 0;
        }

        // xxx make this protected
        static void setBlocksize(size_t newSize) {
            blocksizeRef() = newSize;
        }

    private:

        static size_t& blocksizeRef() {
            static size_t theBlocksize = 2 * 1024 * 1024;
            return theBlocksize;
        }

        char* m_data;
        size_t m_usedBytes;
    };

    class LargeTempTableBlockCache {
    public:
        LargeTempTableBlockCache();

        std::pair<int64_t, LargeTempTableBlock*> getEmptyBlock();

        void unpinBlock(int64_t blockId);

        LargeTempTableBlock* fetchBlock(int64_t blockId);

        void releaseBlock(int64_t blockId);

    private:

        static const int NUM_CACHE_ENTRIES = 25;

        int64_t getNextId() {
            int64_t nextId = m_nextId;
            ++m_nextId;
            return nextId;
        }

        std::array<LargeTempTableBlock, NUM_CACHE_ENTRIES> m_cache;

        std::vector<LargeTempTableBlock*> m_emptyEntries;

        std::map<int64_t, LargeTempTableBlock*> m_liveEntries;

        std::list<int64_t> m_unpinnedEntries;

        int64_t m_nextId;
    };
}

#endif // VOLTDB_LARGETEMPTABLEBLOCKCACHE_H
