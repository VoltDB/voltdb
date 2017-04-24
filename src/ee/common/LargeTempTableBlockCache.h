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

namespace voltdb {

    class LargeTempTable;

    // xxx This class really belongs in storage
    class LargeTempTableBlockCache {
    public:
        LargeTempTableBlockCache();

        std::pair<int64_t, LargeTempTableBlock*> getEmptyBlock(LargeTempTable* ltt);

        void unpinBlock(int64_t blockId);

        LargeTempTableBlock* fetchBlock(int64_t blockId);

        void releaseBlock(int64_t blockId);

        void increaseAllocatedMemory(int64_t numBytes);

    private:

        static const int NUM_CACHE_ENTRIES = 25;

        int64_t getNextId() {
            int64_t nextId = m_nextId;
            ++m_nextId;
            return nextId;
        }

        std::vector<std::unique_ptr<LargeTempTableBlock>> m_cache;

        /* std::vector<LargeTempTableBlock*> m_emptyEntries; */

        std::map<int64_t, LargeTempTableBlock*> m_liveEntries;

        /* std::list<int64_t> m_unpinnedEntries; */

        int64_t m_nextId;
        int64_t m_totalAllocatedBytes;
    };
}

#endif // VOLTDB_LARGETEMPTABLEBLOCKCACHE_H
