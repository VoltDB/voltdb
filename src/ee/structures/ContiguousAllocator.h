/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#ifndef CONTIGUOUSALLOCATOR_H_
#define CONTIGUOUSALLOCATOR_H_

#include <cstdlib>

namespace voltdb {

/**
 * Dead simple buffer chain that allocates fixed size buffers and
 * fixed size indivual allocations to consumers within those
 * buffers.
 *
 * Note, there are few checks here when running in release mode.
 */
class ContiguousAllocator {
    struct Buffer {
        Buffer *prev;
        char data[0];
    };
    /**
     * This is the number of elements in use.  The total number of
     * allocated blocks is this number plus the number of free blocks.
     */
    int64_t m_count;
    /**
     * This is the size of a node from this allocator.
     */
    int32_t m_allocSize;
    /** This is the size of a suballocation for this allocator. */
    int32_t m_chunkSize;
    /**
     * This is the Buffer from which we will allocate.  Nodes are carved
     * from the end of this buffer.  When m_count % m_chunkSize reaches
     * zero and we want a new node we allocate a new chunk.  The address
     * of the last node is then m_tail->data + (m_count - 1) * m_allocSize.
     */
    Buffer *m_tail;
    /** This is the number of blocks in this allocation. */
    int32_t m_blockCount;
    /** Should we cache the last deleted block?  This helps not thrash the allocator. */
    bool    m_cacheLast;
    /** This is the last deleted block if it needs to be cached. */
    Buffer *m_cachedBuffer;

public:
    /**
     * @param allocSize is the size in bytes of individual allocations.
     * @param chunkSize is the number of allocations per buffer (not bytes).
     */
    ContiguousAllocator(int32_t allocSize, int32_t chunkSize, bool cacheLast = false);
    ~ContiguousAllocator();

    void *alloc();
    void *last() const;
    void trim();
    int64_t count() const { return m_count; }

    int32_t allocationSize() const { return m_allocSize; }

    size_t bytesAllocated() const;

    /** Do we have a cached last buffer?  This is used in testing. */
    bool hasCachedLastBuffer() const { return (m_cachedBuffer != NULL); }
    /** Are we caching the last buffer?  This is used in testing. */
    bool isCachingLastBuffer() const { return m_cacheLast; }
};

} // namespace voltdb

#endif // CONTIGUOUSALLOCATOR_H_
