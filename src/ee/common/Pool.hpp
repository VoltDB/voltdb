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

#ifndef MEMCHECK
/**
 * Description of a chunk of memory allocated on the heap
 */
class Chunk {
public:
    Chunk()
        : m_offset(0), m_size(0), m_chunkData(NULL)
    {
    }

    inline Chunk(uint64_t size, void *chunkData)
        : m_offset(0), m_size(size), m_chunkData(static_cast<char*>(chunkData))
    {
    }

    int64_t getSize() const
    {
        return static_cast<int64_t>(m_size);
    }

    uint64_t m_offset;
    uint64_t m_size;
    char *m_chunkData;
};

/*
 * Find next higher power of two
 * From http://en.wikipedia.org/wiki/Power_of_two
 */
template <class T>
inline T nexthigher(T k) {
        if (k == 0)
                return 1;
        k--;
        for (int i=1; i<sizeof(T)*CHAR_BIT; i<<=1)
                k = k | k >> i;
        return k+1;
}

/**
 * A memory pool that provides fast allocation and deallocation. The
 * only way to release memory is to free all memory in the pool by
 * calling purge.
 */
class Pool {
public:

    Pool() :
        m_allocationSize(TEMP_POOL_CHUNK_SIZE), m_maxChunkCount(1), m_currentChunkIndex(0)
    {
        init();
    }

    Pool(uint64_t allocationSize, uint64_t maxChunkCount) :
#ifdef USE_MMAP
        m_allocationSize(nexthigher(allocationSize)),
#else
        m_allocationSize(allocationSize),
#endif
        m_maxChunkCount(static_cast<std::size_t>(maxChunkCount)),
        m_currentChunkIndex(0)
    {
        init();
    }

    void init() {
#ifdef USE_MMAP
        char *storage =
                static_cast<char*>(::mmap( 0, m_allocationSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0 ));
        if (storage == MAP_FAILED) {
            std::cout << strerror( errno ) << std::endl;
            throwFatalException("Failed mmap");
        }
#else
        char *storage = new char[m_allocationSize];
#endif
        m_chunks.push_back(Chunk(m_allocationSize, storage));
    }

    ~Pool() {
        for (std::size_t ii = 0; ii < m_chunks.size(); ii++) {
#ifdef USE_MMAP
            if (::munmap( m_chunks[ii].m_chunkData, m_chunks[ii].m_size) != 0) {
                std::cout << strerror( errno ) << std::endl;
                throwFatalException("Failed munmap");
            }
#else
            delete [] m_chunks[ii].m_chunkData;
#endif
        }
        for (std::size_t ii = 0; ii < m_oversizeChunks.size(); ii++) {
#ifdef USE_MMAP
            if (::munmap( m_oversizeChunks[ii].m_chunkData, m_oversizeChunks[ii].m_size) != 0) {
                std::cout << strerror( errno ) << std::endl;
                throwFatalException("Failed munmap");
            }
#else
            delete [] m_oversizeChunks[ii].m_chunkData;
#endif
        }
    }

    /*
     * Allocate a continous block of memory of the specified size.
     */
    inline void* allocate(std::size_t size) {
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
#ifdef USE_MMAP
                char *storage =
                        static_cast<char*>(::mmap( 0, nexthigher(size), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0 ));
                if (storage == MAP_FAILED) {
                    std::cout << strerror( errno ) << std::endl;
                    throwFatalException("Failed mmap");
                }
#else
                char *storage = new char[size];
#endif
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
            } else {
                /*
                 * Need to allocate a new chunk
                 */
//                std::cout << "Pool had to allocate a new chunk. Not a good thing "
//                  "from a performance perspective. If you see this we need to look "
//                  "into structuring our pool sizes and allocations so the this doesn't "
//                  "happen frequently" << std::endl;
#ifdef USE_MMAP
                char *storage =
                        static_cast<char*>(::mmap( 0, m_allocationSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0 ));
                if (storage == MAP_FAILED) {
                    std::cout << strerror( errno ) << std::endl;
                    throwFatalException("Failed mmap");
                }
#else
                char *storage = new char[m_allocationSize];
#endif
                m_chunks.push_back(Chunk(m_allocationSize, storage));
                Chunk &newChunk = m_chunks.back();
                newChunk.m_offset = size;
                return newChunk.m_chunkData;
            }
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

    /*
     * Allocate a continous block of memory of the specified size conveniently initialized to 0s
     */
    inline void* allocateZeroes(std::size_t size) { return ::memset(allocate(size), 0, size); }

    inline void purge() {
        /*
         * Erase any oversize chunks that were allocated
         */
        const std::size_t numOversizeChunks = m_oversizeChunks.size();
        for (std::size_t ii = 0; ii < numOversizeChunks; ii++) {
#ifdef USE_MMAP
            if (::munmap( m_oversizeChunks[ii].m_chunkData, m_oversizeChunks[ii].m_size) != 0) {
                std::cout << strerror( errno ) << std::endl;
                throwFatalException("Failed munmap");
            }
#else
            delete [] m_oversizeChunks[ii].m_chunkData;
#endif
        }
        m_oversizeChunks.clear();

        /*
         * Set the current chunk to the first in the list
         */
        m_currentChunkIndex = 0;
        std::size_t numChunks = m_chunks.size();

        /*
         * If more then maxChunkCount chunks are allocated erase all extra chunks
         */
        if (numChunks > m_maxChunkCount) {
            for (std::size_t ii = m_maxChunkCount; ii < numChunks; ii++) {
#ifdef USE_MMAP
                if (::munmap( m_chunks[ii].m_chunkData, m_chunks[ii].m_size) != 0) {
                    std::cout << strerror( errno ) << std::endl;
                    throwFatalException("Failed munmap");
                }
#else
                delete []m_chunks[ii].m_chunkData;
#endif
            }
            m_chunks.resize(m_maxChunkCount);
        }

        numChunks = m_chunks.size();
        for (std::size_t ii = 0; ii < numChunks; ii++) {
            m_chunks[ii].m_offset = 0;
        }
    }

    int64_t getAllocatedMemory()
    {
        int64_t total = 0;
        total += m_chunks.size() * m_allocationSize;
        for (int i = 0; i < m_oversizeChunks.size(); i++)
        {
            total += m_oversizeChunks[i].getSize();
        }
        return total;
    }

private:
    const uint64_t m_allocationSize;
    std::size_t m_maxChunkCount;
    std::size_t m_currentChunkIndex;
    std::vector<Chunk> m_chunks;
    /*
     * Oversize chunks that will be freed and not reused.
     */
    std::vector<Chunk> m_oversizeChunks;
    // No implicit copies
    Pool(const Pool&);
    Pool& operator=(const Pool&);
};
#else
/**
 * A debug version of the memory pool that does each allocation on the heap keeps a list for when purge is called
 */
class Pool {
public:
    Pool()
    {
    }

    Pool(uint64_t allocationSize, uint64_t maxChunkCount) :
        m_memTotal(0)
    {
    }

    ~Pool() {
        purge();
    }

    /*
     * Allocate a continous block of memory of the specified size.
     */
    inline void* allocate(std::size_t size) {
        char *retval = new char[size];
        m_allocations.push_back(retval);
        m_memTotal += size;
        return retval;
    }

    /*
     * Allocate a continous block of memory of the specified size conveniently initialized to 0s
     */
    inline void* allocateZeroes(std::size_t size) { return ::memset(allocate(size), 0, size); }

    inline void purge() {
        for (std::size_t ii = 0; ii < m_allocations.size(); ii++) {
            delete [] m_allocations[ii];
        }
        m_allocations.clear();
        m_memTotal = 0;
    }

    int64_t getAllocatedMemory()
    {
        return m_memTotal;
    }

private:
    std::vector<char*> m_allocations;
    int64_t m_memTotal;
    // No implicit copies
    Pool(const Pool&);
    Pool& operator=(const Pool&);
};
#endif
}
#endif /* POOL_HPP_ */
