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

#ifndef CONTIGUOUSFASTALLOCATOR_HPP_
#define CONTIGUOUSFASTALLOCATOR_HPP_
#include "boost/pool/pool.hpp"
#include "boost/shared_ptr.hpp"
#include "common/ThreadLocalPool.h"

namespace voltdb {

template <typename T> class ContiguousFastAllocator;

template <> class ContiguousFastAllocator<void> {
public:
  typedef void*       pointer;
  typedef const void* const_pointer;
  // reference to void members are impossible.
  typedef void value_type;
  template <typename U> struct rebind { typedef ContiguousFastAllocator<U>
                                     other; };
};

/**
 * STL compatible allocator that allocates/deallocates from thread local
 * memory pools that serve fixed size allocations. Optimized
 * for allocating contiguous memory for the vectors used by free lists. Instead of paying
 * the overhead of contiguous allocation via ordered_allocate and ordered_free just
 * size the contiguous memory appropriately since the max that will be asked for is bounded
 * and generally speaking the larger sizes will only have a few allocations necessary
 * due to compaction.
 */
template <typename T>
class ContiguousFastAllocator {
public:
    typedef size_t    size_type;
    typedef ptrdiff_t difference_type;
    typedef T*        pointer;
    typedef const T*  const_pointer;
    typedef T&        reference;
    typedef const T&  const_reference;
    typedef T         value_type;
    template <typename U> struct rebind { typedef ContiguousFastAllocator<U>
                                        other; };
    ContiguousFastAllocator() {}

    template <typename U>
    ContiguousFastAllocator( const ContiguousFastAllocator<U> &other) {}
    ContiguousFastAllocator(const ContiguousFastAllocator<T> &other) {}

    static pointer address(reference reference) {
        return &reference;
    }

    static const_pointer address(const_reference reference) {
        return &reference;
    }

    static size_type max_size() {
        return (std::numeric_limits<size_type>::max)();
    }

    void construct(const pointer p, const value_type &val) {
        if (p != NULL) {
            new (p) T(val);
        }
    }

    void destroy(const pointer ptr) {
        if (ptr != NULL) {
            ptr->~T();
        }
    }

    bool operator==(const ContiguousFastAllocator &other) const {
        return true;
    }

    bool operator!=(const ContiguousFastAllocator &other) const {
        return false;
    }

    pointer allocate(const size_type n) {
        if (n == 0) {
            return NULL;
        }
        const pointer ret = (n == 1) ?
                static_cast<pointer>(
                        ThreadLocalPool::getExact(sizeof(T))->malloc()) :
                        static_cast<pointer>(
                                ThreadLocalPool::getExact(sizeof(T) * n)->malloc() );
        if (ret == 0) {
            boost::throw_exception(std::bad_alloc());
        }
        return ret;
    }

    pointer allocate(const size_type n, const void * const) {
        return allocate(n);
    }

    pointer allocate() {
        boost::shared_ptr<boost::pool<boost::default_user_allocator_new_delete> > pool = ThreadLocalPool::getExact(sizeof(T));
        const pointer ret = pool->malloc();
        if (ret == 0) {
            boost::throw_exception(std::bad_alloc());
        }
        return ret;
    }

    void deallocate(const pointer ptr, const size_type n) {
        if (ptr == NULL || n == 0) {
            return;
        }
        if (n == 1) {
            ThreadLocalPool::getExact(sizeof(T))->free(ptr);
        } else {
            ThreadLocalPool::getExact(sizeof(T) * n)->free(ptr);
        }
    }

    void deallocate(const pointer ptr) {
        if (ptr == NULL) {
            return;
        }
        boost::shared_ptr<boost::pool<boost::default_user_allocator_malloc_free> > pool = ThreadLocalPool::getExact(sizeof(T));
        pool->free(ptr);
    }
};
}
#endif /* CONTIGUOUSFASTALLOCATOR_HPP_ */
