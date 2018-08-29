/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

#pragma once
#include <cstdio>
#include <cstring>
#include <cstdint>
#include <vector>
#include <deque>
#include <atomic>

namespace voltdb {

   static const size_t TEMP_POOL_CHUNK_SIZE = 262144;
#ifndef MEMCHECK
   /**
    * A memory pool that provides fast allocation and deallocation. The
    * only way to release memory is to free all memory in the pool by
    * calling purge.
    */
   class Pool {
      /**
       * Description of a chunk of memory allocated on the heap
       */
      struct Chunk {
         uint64_t m_offset = 0;
         uint64_t m_size = 0;
         char *m_chunkData = nullptr;
         Chunk() {}
         Chunk(uint64_t size, void *chunkData)
            : m_size(size), m_chunkData(static_cast<char*>(chunkData)) { }
         int64_t getSize() const {
            return m_size;
         }
      };
      const uint64_t m_allocationSize = TEMP_POOL_CHUNK_SIZE;
      std::size_t m_maxChunkCount = 1;
      std::size_t m_currentChunkIndex = 0;
      std::vector<Chunk> m_chunks;
      /*
       * Oversize chunks that will be freed and not reused.
       */
      std::vector<Chunk> m_oversizeChunks;
      // No implicit copies
      Pool(const Pool&);
      Pool& operator=(const Pool&);
   public:
      Pool();
      Pool(uint64_t allocationSize, uint64_t maxChunkCount);
      void init();
      virtual ~Pool();
      /*
       * Allocate a continous block of memory of the specified size.
       */
      void* allocate(std::size_t size);
      /*
       * Allocate a continous block of memory of the specified size conveniently initialized to 0s
       */
      void* allocateZeroes(std::size_t size);
      void purge() throw();
      int64_t getAllocatedMemory() const;
   };
#else // for MEMCHECK builds
   /**
    * A debug version of the memory pool that does each allocation on the heap keeps a list for when purge is called
    */
   class Pool {
      std::vector<char*> m_allocations();
      int64_t m_memTotal = 0;
      // No implicit copies
      Pool(const Pool&);
      Pool& operator=(const Pool&);
   public:
      Pool() {}
      Pool(uint64_t allocationSize, uint64_t maxChunkCount) {}
      virtual ~Pool() {purge();}
      /*
       * Allocate a continous block of memory of the specified size.
       */
      void* allocate(std::size_t size);
      /*
       * Allocate a continous block of memory of the specified size conveniently initialized to 0s
       */
      void* allocateZeroes(std::size_t size);
      void purge() throw();
      int64_t getAllocatedMemory() const;
   };
#endif
   /**
    * Resource pool for all heteregeous VoltAllocs.
    */
   class VoltAllocResourceMng {
   protected:
      static Pool s_VoltAllocatorPool;
      static std::atomic_int s_AllocInUse;
      static void* operator new(std::size_t sz) {
//         fprintf(stderr, "Ask for %lu bytes\n", sz);
         void* p = s_VoltAllocatorPool.allocate(sz);
//         fputs("granted\n", stderr);
         return p;
      }
      static void* operator new(std::size_t sz, void* p) {
         return p;
      }
      static void operator delete(void*) { /* every-day deallocator does nothing -- lets the pool cope */ }
   };
   /**
    * An allocator to be used in lieu with STL containers, but
    * allocate from a global memory pool.
    * e.g. std::vector<TxnMem, VoltAlloc<TxnMem>> s;
    */
   template<typename T> class allocator: public VoltAllocResourceMng {
      public:
         typedef size_t     size_type;
         typedef ptrdiff_t  difference_type;
         typedef T*       pointer;
         typedef const T* const_pointer;
         typedef T&       reference;
         typedef const T& const_reference;
         typedef T        value_type;
         template<typename Tp1> struct rebind { typedef allocator<Tp1> other; };

         allocator() throw() { }
         allocator(const allocator&) throw() { }
         template<typename Tp1> allocator(const allocator<Tp1>&) throw() { }
         ~allocator() throw() { }
         pointer address(reference x) const throw() { return std::addressof(x); }
         const_pointer address(const_reference x) const throw() { return std::addressof(x); }
         pointer allocate(size_type n, const void* = static_cast<const void*>(nullptr)) {
            if (n > this->max_size())
               throw std::bad_alloc();
            return static_cast<T*>(VoltAllocResourceMng::operator new(n * sizeof(T)));
         }
         void deallocate(pointer p, size_type) {
            VoltAllocResourceMng::operator delete(p);
         }
         size_type max_size() const throw() { return size_t(-1) / sizeof(T); }
//         void construct(pointer p, const T& val) { new((void *)p) T(val); }
         template<typename U> void destroy(U* p) { p->~U(); }
   };

   template<typename T1, typename T2> inline bool operator==(const allocator<T1>&, const allocator<T2>&) throw() { return true; }
   template<typename T> inline bool operator==(const allocator<T>&, const allocator<T>&) throw() { return true; }

   template<typename T1, typename T2> inline bool operator!=(const allocator<T1>&, const allocator<T2>&) throw() { return false; }
   template<typename T> inline bool operator!=(const allocator<T>&, const allocator<T>&) throw() { return false; }

}
