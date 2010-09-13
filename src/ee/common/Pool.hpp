/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef POOL_HPP_
#define POOL_HPP_
#include <vector>
#include <iostream>
#include <stdint.h>

namespace voltdb {
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

    uint64_t m_offset;
    uint64_t m_size;
    char *m_chunkData;
};

/**
 * A memory pool that provides fast allocation and deallocation. The
 * only way to release memory is to free all memory in the pool by
 * calling purge.
 */
class Pool {
public:
    Pool() :
        m_allocationSize(65536), m_maxChunkCount(1), m_currentChunkIndex(0)
    {
        m_chunks.push_back(Chunk(m_allocationSize, new char[m_allocationSize]));
    }

    Pool(uint64_t allocationSize, uint64_t maxChunkCount) :
        m_allocationSize(allocationSize),
        m_maxChunkCount(static_cast<std::size_t>(maxChunkCount)),
        m_currentChunkIndex(0)
    {
        m_chunks.push_back(Chunk(allocationSize, new char[allocationSize]));
    }

    ~Pool() {
        for (std::size_t ii = 0; ii < m_chunks.size(); ii++) {
            delete [] m_chunks[ii].m_chunkData;
        }
        for (std::size_t ii = 0; ii < m_oversizeChunks.size(); ii++) {
            delete [] m_oversizeChunks[ii].m_chunkData;
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
                m_oversizeChunks.push_back(Chunk(size, new char[size]));
                Chunk *newChunk = &(*(m_oversizeChunks.end()));
                newChunk->m_offset = size;

                return newChunk->m_chunkData;
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
                m_chunks.push_back(Chunk(m_allocationSize, new char[m_allocationSize]));
                currentChunk = &(*(m_chunks.rbegin()));
                currentChunk->m_offset = size;
                return currentChunk->m_chunkData;
            }
        }

        /*
         * Get the offset into the current chunk. Then increment the
         * offset counter by the amount being allocated.
         */
        void *retval = &currentChunk->m_chunkData[currentChunk->m_offset];
        currentChunk->m_offset += size;

        //Ensure 8 byte alignment of future allocations
        currentChunk->m_offset += (8 - (currentChunk->m_offset % 8));
        if (currentChunk->m_offset > currentChunk->m_size) {
            currentChunk->m_offset = currentChunk->m_size;
        }

        return retval;
    }

    inline void purge() {
        /*
         * Erase any oversize chunks that were allocated
         */
        const std::size_t numOversizeChunks = m_oversizeChunks.size();
        for (std::size_t ii = 0; ii < numOversizeChunks; ii++) {
            delete [] m_oversizeChunks[ii].m_chunkData;
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
                delete [] m_chunks[ii].m_chunkData;
            }
            m_chunks.resize(m_maxChunkCount);
        }

        numChunks = m_chunks.size();
        for (std::size_t ii = 0; ii < numChunks; ii++) {
            m_chunks[ii].m_offset = 0;
        }
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

    Pool(uint64_t allocationSize, uint64_t maxChunkCount)
    {
    }

    ~Pool() {
        for (std::size_t ii = 0; ii < m_allocations.size(); ii++) {
            delete [] m_allocations[ii];
        }
        m_allocations.clear();
    }

    /*
     * Allocate a continous block of memory of the specified size.
     */
    inline void* allocate(std::size_t size) {
        char *retval = new char[size];
        m_allocations.push_back(retval);
        return retval;
    }

    inline void purge() {
        for (std::size_t ii = 0; ii < m_allocations.size(); ii++) {
            delete [] m_allocations[ii];
        }
        m_allocations.clear();
    }

private:
    std::vector<char*> m_allocations;
    // No implicit copies
    Pool(const Pool&);
    Pool& operator=(const Pool&);
};
#endif
}
#endif /* POOL_HPP_ */
