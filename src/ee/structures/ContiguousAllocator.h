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

#ifndef CONTIGUOUSALLOCATOR_H_
#define CONTIGUOUSALLOCATOR_H_

#include <cstdlib>

namespace voltdb {

/**
 * This is a dead simple buffer chain that allocates fixed size buffers and
 * parcels out smaller, fixed size individual allocations to consumers.  These
 * allocations are carved out of the fixed size buffers.  Allocations
 * are allocated and returned in LIFO order.  So, if an allocation is
 * deleted from a data structure, the data from the most recent allocation
 * must to be copied to the deleted allocation, so that the most recent
 * allocation's data may be recovered.  The clients all do this.
 *
 * A *block* is a fixed size allocation, which has been obtained from
 * malloc(3). These are chained together.  They are all the same size
 * in bytes.  This size is set when the allocator is constructed.
 *
 * The head of the chain of blocks is the *tail block*.  Blocks which
 * are not the tail block are completely full.
 *
 * An *allocation* is a hunk of memory which is returned to a client of this class.
 * Its size is set when the allocator is constructed.
 *
 * Note, there are few checks here when running in release mode.
 */
class ContiguousAllocator {
    /*
     * Each block is one of these.  Blocks are chained using the
     * prev field.
     */
    struct Buffer {
        Buffer *prev;
        char data[0];
    };
    /** This is the total number of allocations in use in all blocks. */
    int64_t m_count;
    /** This is the size of a allocation for this allocator in bytes. */
    const int32_t m_allocationSize;
    /** This is the number of allocations in each block in this allocator. */
    const int32_t m_numberAllocationsPerBlock;
    /**
     * This points to the tail buffer.  When m_count % m_chunkSize reaches
     * zero and we want a new node we must allocate a new block.  The address
     * of the last node is then m_tail->data + (m_count - 1) * m_allocSize.
     */
    Buffer *m_tail;
    /** This is the number of blocks in this allocation. */
    int32_t m_blockCount;
    /**
     * This is the last deleted block.  If all the allocations are
     * returned, then we cached the last block here, to avoid thrashing
     * the allocator.
     */
    Buffer *m_cachedBuffer;

public:

    /**
     * @param allocSize is the size in bytes of individual allocations.
     * @param chunkSize is the number of allocations per block (not bytes).
     */
    ContiguousAllocator(int32_t allocSize, int32_t chunkSize);
    ~ContiguousAllocator();

    /**
     * Return an allocation.
     */
    void *alloc();

    /**
     * Return the address of the last allocation.   It is an error if this function
     * is called when there are no used allocations.
     */
    void *last() const;

    /**
     * Recover the last allocation.  The data in the last allocation must
     * be copied someplace else or it will be lost.
     */
    void trim();

    /**
     * Return the number of used allocations in this allocator.  This may
     * be zero.
     */
    int64_t count() const { return m_count; }

    /**
     * Return the size of an allocation.
     */
    int32_t allocationSize() const { return m_allocationSize; }

    /**
     * Return the number of bytes allocated to allocations.  This does not
     * include overhead for the block chain pointers.
     */
    size_t bytesAllocated() const;

    /** Do we have a cached last buffer?  This is used in testing. */
    bool hasCachedLastBuffer() const { return (m_cachedBuffer != NULL); }
};

} // namespace voltdb

#endif // CONTIGUOUSALLOCATOR_H_
