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

#include "Pool.hpp"
#include <sys/mman.h>
#include <errno.h>
#include <algorithm>
#include <iostream>
#include <climits>
#include <numeric>
#include <cstring>
#include "common/FatalException.hpp"

using namespace voltdb;

#ifndef MEMCHECK

Pool::Pool(): m_chunkSize(TEMP_POOL_CHUNK_SIZE), m_maxChunkCount(1), m_currentChunkIndex(0), m_oversizeChunkSize(0) {
   init();
}

Pool::Pool(uint64_t chunkSize, uint64_t maxChunkCount) :
   m_chunkSize(chunkSize),
   m_maxChunkCount(maxChunkCount), m_currentChunkIndex(0), m_oversizeChunkSize(0)
{
   init();
}

void Pool::init() {
   m_chunks.reserve(m_maxChunkCount);
   m_chunks.emplace_back(m_chunkSize, 0);
}

Pool::~Pool() {
}

void* Pool::allocate(std::size_t size) {
   /*
    * See if there is space in the current chunk
    */
   Chunk &currentChunk = m_chunks[m_currentChunkIndex];
   const bool fitsInCurrentChunk = size + currentChunk.offset() <= currentChunk.size(),
         fitsInNormalChunk = size <= m_chunkSize;
   if (! fitsInCurrentChunk) {
      /*
       * Not enough space. Check if it is greater then our allocation size per chunk.
       */
      if (! fitsInNormalChunk) {
         /*
          * Allocate an oversize chunk that will not be reused,
          * i.e. reclaimed in call to purge() method.
          */
         m_oversizeChunks.emplace_back(new char[size]);
         m_oversizeChunkSize += size;
         return m_oversizeChunks.back().get();
      } else {   // fits in normal chunk size - check if there is an already allocated chunk we can use.
         m_currentChunkIndex++;
         if (m_currentChunkIndex < m_chunks.size()) {   // Some chunks from pre-allocated (a.k.a. init()-ed) have not been used yet.
            Chunk& currentChunk = m_chunks.at(m_currentChunkIndex);
            currentChunk.offset() = size;
            return currentChunk.data();
         } else {                                       // Need to allocate a new chunk
            if (m_currentChunkIndex > m_maxChunkCount) {
               VOLT_WARN("%s\n",
                     "Pool had to allocate a new chunk. Not a good thing "
                     "from a performance perspective. If you see this we need to look "
                     "into structuring our pool sizes and allocations so the this doesn't "
                     "happen frequently");
            }
            m_chunks.emplace_back(m_chunkSize, size);    // and adjust chunk's offset
            return m_chunks.back().data();
         }
      }
   } else {   // fits in last normal chunk
      /*
       * Get the offset into the current chunk. Then increment the
       * offset counter by the amount being allocated.
       */
      char *retval = currentChunk.data() + currentChunk.offset();
      currentChunk.offset() += size;
      //Ensure 8 byte alignment of future allocations. 8-byte
      //alignment is always done by struct/class, but not for C-array.
      currentChunk.offset() += currentChunk.padding();
      // and ensure offset doesn't go beyond
      currentChunk.offset() = std::min<uint64_t>(currentChunk.offset(), currentChunk.size());
      return retval;
   }
}

void* Pool::allocateZeroes(std::size_t size) {
   return std::memset(allocate(size), 0, size);
}

void Pool::purge() throw() {
   /*
    * Erase any oversize chunks that were allocated
    */
   m_oversizeChunks.clear();
   m_oversizeChunkSize = 0;

   /*
    * Set the current chunk to the first in the list
    */
   m_currentChunkIndex = 0;
   std::size_t numChunks = m_chunks.size();

   /*
    * If more than maxChunkCount chunks are allocated erase all extra chunks
    */
   if (numChunks > m_maxChunkCount) {
      m_chunks.erase(std::next(m_chunks.begin(), m_maxChunkCount), m_chunks.end());
   }
#ifdef MODERN_CXX
   std::for_each(m_chunks.begin(), m_chunks.end(), [](Chunk& cur) { cur.offset() = 0; });
#else
   for(std::vector<Chunk>::iterator iter = m_chunks.begin(); iter != m_chunks.end(); ++iter) {
      iter->offset() = 0;
   }
#endif
}

#else // for MEMCHECK builds

void* Pool::allocate(std::size_t size) {
   m_allocations.emplace_back(std::unique_ptr<char[]>(new char[size]));
   m_memTotal += size;
   return m_allocations.back().get();
}
void* Pool::allocateZeroes(std::size_t size) {
   return ::memset(allocate(size), 0, size);
}
void Pool::purge() throw() {
   m_allocations.clear();
   m_memTotal = 0;
}

#endif
