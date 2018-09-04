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
#include <typeinfo>
#include <cstdio>
#include <cstring>
#include <cstdint>
#include <vector>
#include <memory>
#include <signal.h>     // for sig_atomic_t typedef
#include <mutex>

#if defined __GNUC__ && __GNUC__ >= 5
#define MODERN_CXX
#else
#undef MODERN_CXX
#endif

namespace voltdb {

   static const size_t TEMP_POOL_CHUNK_SIZE = 262144;
#ifndef MEMCHECK
   /**
    * A memory pool that provides fast allocation and deallocation. The
    * only way to release memory is to free all memory in the pool by
    * calling purge.
    *
    * A pool is **NOT** thread-safe, meaning that multiple
    * threads should not call allocate() method from the same
    * Pool instance.
    */
   class Pool {
      /**
       * A chunk of memory allocated on the heap
       */
      class Chunk {
         uint64_t m_offset;
         uint64_t m_size;
         std::unique_ptr<char[]> m_chunkData;
         Chunk& operator=(const Chunk&);
         Chunk(const Chunk&);
      public:
         Chunk(uint64_t size, uint64_t offset)
            : m_offset(offset), m_size(size), m_chunkData(std::unique_ptr<char[]>(new char[size])) { }
         Chunk(Chunk&& rhs): m_offset(rhs.m_offset), m_size(rhs.m_size), m_chunkData(std::move(rhs.m_chunkData)) {
            rhs.m_chunkData.release();
         }
         Chunk& operator=(Chunk&& rhs) {
            m_offset = rhs.m_offset;
            m_size = rhs.m_size;
            m_chunkData.swap(rhs.m_chunkData);
            return *this;
         }
         int64_t size() const throw() {
            return m_size;
         }
         uint64_t offset() const throw () {
            return m_offset;
         }
         uint64_t& offset() throw() {
            return m_offset;
         }
         const char* data() const throw() {
            return m_chunkData.get();
         }
         char* data() throw() {
            return m_chunkData.get();
         }
         size_t padding() const throw() {   // number of bytes needed to advance offset to make next object
            const size_t padding = 8 - (offset() % 8);    // align in 8-byte memory location.
            return padding < 8 ? padding : 0;
         }
      };
#ifndef MODERN_CXX
      /**
       * Note: We need this only for the support of Centos6.
       */
      struct AllocMemoryCalculator {
         int64_t operator()(int64_t acc, const Chunk& cur) {
            return acc + cur.size();
         }
      };
#endif
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
   public:
      Pool();
      Pool(uint64_t allocationSize, uint64_t maxChunkCount);
      void init();
      ~Pool();
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
      std::vector<std::unique_ptr<char[]>> m_allocations;
      int64_t m_memTotal;
      // No implicit copies
      Pool(const Pool&);
      Pool& operator=(const Pool&);
   public:
      Pool() : m_memTotal(0) {}
      Pool(uint64_t allocationSize, uint64_t maxChunkCount) {}
      ~Pool() {purge();}
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
    * Resource pool for all heteregeous VoltAllocs. Only one
    * global pool is used, and all new() operations are blocking,
    * so that at most one thread calls Pool::allocate() method. I
    * found it exactly the same as using std::new()/delete()
    * method.
    *
    * NOTE: another use case for the allocator is to allocate
    * from a given pool, rather than the global allocator pool.
    * This however, would require the allocator to be stateful,
    * which needs full C++11 support. Therefore, I leave it till
    * we migrate off from any C++11-partial-supported
    * compilers/platforms.
    */
   class VoltAllocResourceMng {
      static Pool s_VoltAllocatorPool;
      static volatile sig_atomic_t s_numInstances;
      static std::mutex s_allocMutex;
   public:
      static void* operator new(std::size_t sz) {
         std::lock_guard<std::mutex> lock(s_allocMutex);   // C++11 has thread_local keyword, so no need for locking
         ++s_numInstances;
         return s_VoltAllocatorPool.allocate(sz);
      }
      static void* operator new(std::size_t sz, void* p) {
         return p;
      }
      static void operator delete(void*) {
         if (0 == --s_numInstances) {
            std::lock_guard<std::mutex> lock(s_allocMutex);
            s_VoltAllocatorPool.purge();
         }
      }
   };
   /**
    * An allocator to be used in lieu with STL containers, but
    * allocate from a global memory pool.
    * e.g. std::vector<TxnMem, VoltAlloc<TxnMem>> s;
    *
    * The voltdb::allocator (conceptually) uses a common thread-local Pool,
    * which means that:
    *
    * 1. The allocator is thread-safe, i.e. multiple threads
    * using allocator should be free to use it in any manner;
    *
    * 2. One thread's memory is invisible to the other, meaning
    * that they cannot access the same object in any manner.
    */
   template<typename T> class allocator {
   public:
      typedef std::size_t     size_type;
      typedef std::ptrdiff_t  difference_type;
      typedef T*       pointer;
      typedef const T* const_pointer;
      typedef T&       reference;
      typedef const T& const_reference;
      typedef T        value_type;
      template<typename R> struct rebind { typedef allocator<R> other; };

      allocator() throw() { }
      allocator(const allocator&) throw() { }
      template<typename R> allocator(const allocator<R>&) throw() { }
      ~allocator() throw() { }
#ifdef MODERN_CXX
      pointer address(reference x) const throw() { return std::addressof(x); }
      const_pointer address(const_reference x) const throw() { return std::addressof(x); }
#else
      pointer address(reference x) const throw() { return &x; }
      const_pointer address(const_reference x) const throw() { return &x; }
#endif
      pointer allocate(size_type n, const void* = NULL) {
         if (n > this->max_size())
            throw std::bad_alloc();
         return static_cast<T*>(VoltAllocResourceMng::operator new(n * sizeof(T)));
      }
      void deallocate(pointer p, size_type n) {
         VoltAllocResourceMng::operator delete(p);
      }
      size_type max_size() const throw() { return size_t(-1) / sizeof(T); }
      void construct(pointer p, const T& val) {
         new((void *)p) T(val);       // NOTE: this calls placement new
      }
      template<typename R> void destroy(R* p) {
         p->~R();
      }
   };
   template<typename T1, typename T2> inline bool operator==(const allocator<T1>&, const allocator<T2>&) throw() { return true; }
   template<typename T> inline bool operator==(const allocator<T>&, const allocator<T>&) throw() { return true; }
   template<typename T1, typename T2> inline bool operator!=(const allocator<T1>&, const allocator<T2>&) throw() { return false; }
   template<typename T> inline bool operator!=(const allocator<T>&, const allocator<T>&) throw() { return false; }

   /**
    * Pattern for static factory of creating arbitrary C++ class
    * on the thread-local memory pool.
    * Usage:
    * class Foo {
    *    Foo(Arg1 arg1, Arg2 arg2);
    * public:
    *    static std::unique_ptr<Foo> createInstance(Arg1 arg1, Arg2 arg2) {
    *        return InstanceFactory<Foo>::create(arg1, arg2);
    *    }
    * };
    */
   template<typename T> struct InstanceFactory: private VoltAllocResourceMng {
#ifdef MODERN_CXX
      template<typename... Args> static std::unique_ptr<T> create(Args... args) {
         return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
      }
#else
      // no variadic template argument support...
       static std::unique_ptr<T> create() {
          return std::unique_ptr<T>(new T());
       }
       template<typename Arg1> static std::unique_ptr<T> create(Arg1 arg1) {
          return std::unique_ptr<T>(new T(arg1));
       }
       template<typename Arg1, typename Arg2>
       static std::unique_ptr<T> create(Arg1 arg1, Arg2 arg2) {
          return std::unique_ptr<T>(new T(arg1, arg2));
       }
       template<typename Arg1, typename Arg2, typename Arg3>
       static std::unique_ptr<T> create(Arg1 arg1, Arg2 arg2, Arg3 arg3) {
          return std::unique_ptr<T>(new T(arg1, arg2, arg3));
       }
       template<typename Arg1, typename Arg2, typename Arg3, typename Arg4>
       static std::unique_ptr<T> create(Arg1 arg1, Arg2 arg2, Arg3 arg3, Arg4 arg4) {
          return std::unique_ptr<T>(new T(arg1, arg2, arg3, arg4));
       }
       template<typename Arg1, typename Arg2, typename Arg3, typename Arg4, typename Arg5>
       static std::unique_ptr<T> create(Arg1 arg1, Arg2 arg2, Arg3 arg3, Arg4 arg4, Arg5 arg5) {
          return std::unique_ptr<T>(new T(arg1, arg2, arg3, arg4, arg5));
       }
#endif
   };

}
