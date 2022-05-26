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

#ifndef VOLTDB_LARGETEMPTABLEBLOCKCACHE_H
#define VOLTDB_LARGETEMPTABLEBLOCKCACHE_H

#include <deque>
#include <list>
#include <map>
#include <utility>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/scoped_array.hpp>

#include "storage/LargeTempTableBlock.h"
#include "common/LargeTempTableBlockId.hpp"
#include "common/types.h"

class LargeTempTableTest_OverflowCache;

namespace voltdb {

class Topend;
class TupleSchema;

/**
 * There is one instance of this class for each EE instance (one per
 * thread).
 *
 * This class keeps track of tuple blocks (and associated pools
 * containing variable length data) for all large temp tables
 * currently in use.
 */
class LargeTempTableBlockCache {

    friend class ::LargeTempTableTest_OverflowCache;

 public:

    /**
     * Construct an instance of a cache containing zero large temp
     * table blocks.
     */
    LargeTempTableBlockCache(Topend* topend,
                             int64_t maxCacheSizeInBytes,
                             LargeTempTableBlockId::siteId_t siteId);

    /**
     * A do-nothing destructor
     */
    ~LargeTempTableBlockCache();

    /** Get a new empty large temp table block. */
    LargeTempTableBlock* getEmptyBlock(const TupleSchema* schema);

    /** "Unpin" the specified block, i.e., mark it as a candidate to
        store to disk when the cache becomes full. */
    void unpinBlock(LargeTempTableBlockId blockId);

    /** Returns true if the block is pinned. */
    bool blockIsPinned(LargeTempTableBlockId blockId) const;

    /** Fetch (and pin) the specified block, loading it from disk if
        necessary.  */
    LargeTempTableBlock* fetchBlock(LargeTempTableBlockId blockId);

    /** The large temp table for this block is being destroyed, so
        release all resources associated with this block. */
    void releaseBlock(LargeTempTableBlockId blockId);

    /** The block may have changed in-place (e.g., if we sorted it),
        so remove the copy on disk. */
    void invalidateStoredCopy(LargeTempTableBlock* block);

    /** Get the tuple count for the given block.  Does
        not fetch or pin the block. */
    int64_t getBlockTupleCount(LargeTempTableBlockId blockId) {
        auto it = m_idToBlockMap.find(blockId);
        vassert(it != m_idToBlockMap.end());
        return it->second->get()->activeTupleCount();
    }

    /** The number of pinned (blocks currently being inserted into or
        scanned) entries in the cache */
    size_t numPinnedEntries() const {
        size_t cnt = 0;
        BOOST_FOREACH(auto &block, m_blockList) {
            if (block->isPinned()) {
                ++cnt;
            }
        }

        return cnt;
    }

    /** The number of blocks that are cached in memory (as opposed to
        stored on disk) */
    size_t residentBlockCount() const {
        size_t count = 0;
        BOOST_FOREACH(auto &block, m_blockList) {
            if (block->isResident()) {
                ++count;
            }
        }

        return count;
    }

    /** The total number of large temp table blocks, both cached in
        memory and stored on disk */
    size_t totalBlockCount() const {
        return m_blockList.size();
    }

    /** The number of bytes in blocks that are cached in memory */
    int64_t allocatedMemory() const {
        return m_totalAllocatedBytes;
    }

    /** The max size that the cache can grow to.  If we insert a tuple
        or allocate a new block and exceed this amount, we need to
        store an unpinned block to disk. */
    int64_t maxCacheSizeInBytes() const {
        return m_maxCacheSizeInBytes;
    }

    int maxCacheSizeInBlocks() const {
        return m_maxCacheSizeInBytes / LargeTempTableBlock::BLOCK_SIZE_IN_BYTES;
    }

    /** Release all large temp table blocks (both resident and stored
        on disk) */
    void releaseAllBlocks();

    /** Return a string containing useful debug information */
    std::string debug() const;

    /** Get the block specified by id if it exists, regardless of
        whether is stored on disk or not.  Used for debugging to show
        the state of all a table's blocks.  Does not throw if the
        specified block does not exist. */
    LargeTempTableBlock* getBlockForDebug(LargeTempTableBlockId id) const {
        auto it = m_idToBlockMap.find(id);
        if (it == m_idToBlockMap.end()) {
            return NULL;
        }

        return (it->second)->get();
    }

    /** Produce a string describing the number of cache hits and misses. */
    std::string statsForDebug() const;

 private:

    // This at some point may need to be unique across the entire cluster
    LargeTempTableBlockId getNextId() {
        LargeTempTableBlockId nextId = m_nextId;
        ++m_nextId;
        return nextId;
    }

    // Stores the least recently used block to disk, if needed,
    // to make room for another block.
    void ensureSpaceForNewBlock();

    Topend * const m_topend;

    const int64_t m_maxCacheSizeInBytes;

    typedef std::list<std::unique_ptr<LargeTempTableBlock>> BlockList;

    // The block list is ordered by the time to the next reference to the block:
    //   Blocks in the front are expected to be referenced in the immediate future
    //   Blocks at the end are expected to be referenced in the distant future
    BlockList m_blockList;
    std::map<LargeTempTableBlockId, BlockList::iterator> m_idToBlockMap;

    LargeTempTableBlockId m_nextId;
    int64_t m_totalAllocatedBytes;

    /** stats: */
    int64_t m_numCacheMisses; // calls to "fetch" that required a store/load
    int64_t m_numCacheHits; // calls to "fetch" blocks already resident
};

}

#endif // VOLTDB_LARGETEMPTABLEBLOCKCACHE_H
