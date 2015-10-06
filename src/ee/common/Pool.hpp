/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#ifndef POOL_HPP_
#define POOL_HPP_
#include <vector>
#include <iostream>
#include <stdint.h>
#include <sys/mman.h>
#include <errno.h>
#include <climits>
#include <string.h>
#include "common/FatalException.hpp"

namespace voltdb {
static const size_t TEMP_POOL_CHUNK_SIZE = 262144;

/** NormalPoolAllocator provides the base class for Pool,
 *  the temporary memory pool, under normal build conditions.
 *  That is, ifndef MEMCHECK && ifndef USE_MMAP.
 *  It implements the same "protected:" interface as the alternative
 *  experimental base class, MappedMemoryPoolAllocator.
 *  It handles allocation, allocation sizing, and deallocation.
 */
class NormalPoolAllocator {
protected:
    static char* allocateChunk(std::size_t size)
    { return new char[size]; }

    static void deleteChunk(char* data, std::size_t size)
    { delete [] data; }

    // Only the USE_MMAP option cares about rounding up allocations
    static std::size_t roundChunkUp(std::size_t size)
    { return size; }
};


/** MappedMemoryPoolAllocator provides the base class for Pool,
 *  the temporary memory pool, under normal build conditions.
 *  That is, ifndef MEMCHECK && ifdef USE_MMAP.
 *  It implements the same "protected:" interface as the default
 *  base class, NormalPoolAllocator.
 *  It handles allocation, allocation sizing, and deallocation.
 */
class MappedMemoryPoolAllocator {
protected:
    static char* allocateChunk(std::size_t size)
    {
        char *storage =
            static_cast<char*>(::mmap(0, size,
                    PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0 ));
        if (storage == MAP_FAILED) {
            std::cout << strerror(errno) << std::endl;
            throwFatalException("Failed mmap");
        }
        return storage;
    }

    static void deleteChunk(char* data, std::size_t size)
    {
        if (::munmap(data, size) != 0) {
            std::cout << strerror(errno) << std::endl;
            throwFatalException("Failed munmap");
        }
    }

    /*
     * Find next higher power of two
     * From http://en.wikipedia.org/wiki/Power_of_two
     */
    template <class T>
    static T nexthigher(T k)
    {
        if (k == 0) {
            return 1;
        }
        k--;
        for (int i=1; i<sizeof(T)*CHAR_BIT; i<<=1) {
            k = k | k >> i;
        }
        return k+1;
    }

    // Only the USE_MMAP option cares about rounding up
    // to power of two sized allocations.
    static std::size_t roundChunkUp(std::size_t size)
    { return nexthigher(size); }
};


/**
 * A memory pool that provides fast allocation and deallocation. The
 * only way to release memory is to free all memory in the pool by
 * calling purge.
 * WARNING: Any potential pool-allocated object should carefully manage its
 * deallocation interface, so that "operator delete" is never called on it
 * -- OR it can define a public custom "operator delete" as a no-op.
 * An alternative is to make constructors and destructors private and call
 * them only in a factory method. That has the possibly unnecessary side effect
 * of disallowing instances to be declared on the stack or embedded as members
 * or base classes of other classes.
 * If the object has a non-trivial destructor behavior, the Pool purge will
 * expect that the destructor was already called by the application via
 * explicit call like "myPtr->~MyObject()" or via the normal "delete myPtr"
 * on a class with a no-op operator delete defined prior to purging the pool.
 * If a single object class needs to have both pooled instances and non-pooled
 * instances, some kind of cleverness will be required to distinguish non-pooled
 * instances which MUST be deallocated from pooled instances that MUST NOT be
 * deallocated.
 * @See StringRef for an example, though possibly a slightly overly clever one.
 */
class Pool
    // If MEMCHECK is defined (usually based on a build configuration option),
    // the temp memory pool merely consists of a set of normal "new char[]"
    // allocations that are tracked in a vector and deallocated when the pool
    // is purged or deleted. This is to allow valgrind to more closely track
    // individual objects as separate allocations.
    // Otherwise, several pooled objects share allocation chunks which are
    // allocated differently depending on the enabled ...Allocator base class.
    // The MEMCHECK implementation is simple enough not to need an
    // ...Allocator base class at all.
#ifndef MEMCHECK
#ifndef USE_MMAP
    : private NormalPoolAllocator
#else
    : private MappedMemoryPoolAllocator
#endif
#endif
{


    // The MEMCHECK implementation has very little code in common with the
    // others -- just a handful of definitions at the bottom of the class
    // definition, but it DOES implement the same public API after the
    // # - else below.
#ifndef MEMCHECK

    /**
     * Description of a chunk of memory allocated on the heap
     */
    class Chunk {
    public:
        Chunk() : m_offset(0), m_size(0), m_chunkData(NULL) { }

        Chunk(uint64_t size, void *chunkData)
          : m_offset(0)
          , m_size(size)
          , m_chunkData(static_cast<char*>(chunkData))
        { }

        uint64_t m_offset;
        uint64_t m_size;
        char *m_chunkData;
    };

public:

    Pool()
      : m_allocationSize(TEMP_POOL_CHUNK_SIZE)
      , m_maxChunkCount(1)
      , m_currentChunkIndex(0)
    { init(); }

    Pool(uint64_t allocationSize, std::size_t maxChunkCount)
      : m_allocationSize(roundChunkUp(allocationSize))
      , m_maxChunkCount(maxChunkCount)
      , m_currentChunkIndex(0)
    {
        init();
    }

    void init()
    {
        char* storage = allocateChunk(m_allocationSize);
        m_chunks.push_back(Chunk(m_allocationSize, storage));
    }

    ~Pool()
    {
        // Keep nothing in reserve for the final purge.
        m_maxChunkCount = 0;
        purge();
    }

    /*
     * Allocate a continous block of memory of the specified size.
     */
    void* allocate(std::size_t size) {
        /*
         * See if there is space in the current chunk
         */
        Chunk *currentChunk = &m_chunks[m_currentChunkIndex];
        if (size > currentChunk->m_size - currentChunk->m_offset) {
            /*
             * Not enough space. Check if it is greater then our allocation size.
             */
            if (size > m_allocationSize) {
                /*
                 * Allocate an oversize chunk that will not be reused.
                 */
                char *storage = allocateChunk(roundChunkUp(size));
                m_oversizeChunks.push_back(Chunk(nexthigher(size), storage));
                Chunk &newChunk = m_oversizeChunks.back();
                newChunk.m_offset = size;
                return newChunk.m_chunkData;
            }

            /*
             * Check if there is an already allocated chunk we can use.
             */
            m_currentChunkIndex++;
            if (m_currentChunkIndex < m_chunks.size()) {
                currentChunk = &m_chunks[m_currentChunkIndex];
                currentChunk->m_offset = size;
                return currentChunk->m_chunkData;
            }

            /*
             * Need to allocate a new chunk
             */
//                std::cout << "Pool had to allocate a new chunk. Not a good thing "
//                  "from a performance perspective. If you see this we need to look "
//                  "into structuring our pool sizes and allocations so the this doesn't "
//                  "happen frequently" << std::endl;
            char *storage = allocateChunk(m_allocationSize);
            m_chunks.push_back(Chunk(m_allocationSize, storage));
            Chunk &newChunk = m_chunks.back();
            newChunk.m_offset = size;
            return newChunk.m_chunkData;
        }

        /*
         * Get the offset into the current chunk. Then increment the
         * offset counter by the amount being allocated.
         */
        void *retval = currentChunk->m_chunkData + currentChunk->m_offset;
        currentChunk->m_offset += size;

        //Ensure 8 byte alignment of future allocations
        currentChunk->m_offset += (8 - (currentChunk->m_offset % 8));
        if (currentChunk->m_offset > currentChunk->m_size) {
            currentChunk->m_offset = currentChunk->m_size;
        }

        return retval;
    }

    /* Deallocate pooled memory down to a minimum number of standard-sized
     * chunks. These are merely "reset" to empty and kept in reserve for
     * possible future reuse of
     * the Pool.
     * The destructor will eventually clean these up, too.

    void purge()
    {
        /*
         * Erase any oversize chunks that were allocated
         */
        std::size_t nOversizeChunks = m_oversizeChunks.size();
        while (nOversizeChunks--) {
            deleteChunk(m_oversizeChunks[nOversizeChunks].m_chunkData,
                        m_oversizeChunks[nOversizeChunks].m_size);
        }
        m_oversizeChunks.clear();

        /*
         * If more then maxChunkCount chunks are allocated erase all extra chunks
         * and shrink the vector.
         * If maxChunkCount is not yet reached, the vector need not be resized.
         */
        std::size_t nChunks = m_chunks.size();
        if (nChunks > m_maxChunkCount) {
            while (nChunks-- > m_maxChunkCount) {
                deleteChunk(m_chunks[nChunks].m_chunkData, m_chunks[nChunks].m_size);
            }
            m_chunks.resize(m_maxChunkCount);
        }

        /*
         * Set the current chunk to the first in the vector and mark that chunk
         * as empty. Any other preserved chunks are assumed empty when
         * m_currentChunkIndex gets bumped up to reference them.
         */
        m_currentChunkIndex = 0;

        if (m_maxChunkCount > 0) {
            m_chunks[0].m_offset = 0;
        }
    }

    std::size_t getAllocatedMemory() const
    {
        std::size_t total = 0;
        total += m_chunks.size() * m_allocationSize;
        int nOversizeChunks = m_oversizeChunks.size();
        while (nOversizeChunks--) {
            total += m_oversizeChunks[nOversizeChunks].m_size;
        }
        return total;
    }

private:
    const std::size_t m_allocationSize;
    std::size_t m_maxChunkCount;
    std::size_t m_currentChunkIndex;
    std::vector<Chunk> m_chunks;
    /*
     * Oversize chunks that will be freed and not reused.
     */
    std::vector<Chunk> m_oversizeChunks;

#else // MEMCHECK

    /**
     * A valgrind-friendly memory debugging version of the memory pool that
     * does each allocation on the C++ heap and keeps a vector of the allocated
     * pointers, until it deallocates them in the purge method or on Pool
     * destruction. Unlike the other implementations, it does not continue
     * to reserve ANY allocated memory after a purge.
     * The MEMCHECK implementation does not bother with the inherited
     * allocation functions -- it never uses mapped memory and so it
     * can esily call the "Normal" C++ new/delete operators directly.
     */
public:
    Pool() : m_memTotal(0) { }

    Pool(uint64_t allocationSize, std::size_t maxChunkCount) : m_memTotal(0) { }

    ~Pool() { purge(); }

    /*
     * Allocate a continous block of memory of the specified size.
     */
    void* allocate(std::size_t size)
    {
        char *retval = new char[size];
        m_allocations.push_back(retval);
        m_memTotal += size;
        return retval;
    }

    void purge()
    {
        std::size_t nAllocation = m_allocations.size();
        while (nAllocation--) {
            char* alloced = m_allocations[nAllocation];
            delete [] alloced;
        }
        m_allocations.clear();
        m_memTotal = 0;
    }

    std::size_t getAllocatedMemory () const { return m_memTotal; }

private:
    std::vector<char*> m_allocations;
    std::size_t m_memTotal;
#endif

    //
    // Code common to the normal and MEMCHECK versions can go here.
    //

public:
    /*
     * Allocate a continous block of memory of the specified size conveniently initialized to 0s
     */
    void* allocateZeroes(std::size_t size) { return ::memset(allocate(size), 0, size); }
private:
    // No implicit copies
    Pool(const Pool&);
    Pool& operator=(const Pool&);
};

}
#endif /* POOL_HPP_ */
