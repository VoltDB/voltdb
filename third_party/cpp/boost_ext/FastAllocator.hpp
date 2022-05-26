/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
// Copyright (C) 2000, 2001 Stephen Cleary
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//
// See http://www.boost.org for updates, documentation, and revision history.
#ifndef FASTALLOCATOR_HPP_
#define FASTALLOCATOR_HPP_
#include "boost/pool/pool.hpp"
#include "boost/shared_ptr.hpp"
#include "common/ThreadLocalPool.h"

namespace voltdb {

template <typename T> class FastAllocator;

template <> class FastAllocator<void> {
public:
  typedef void*       pointer;
  typedef const void* const_pointer;
  // reference to void members are impossible.
  typedef void value_type;
  template <typename U> struct rebind { typedef FastAllocator<U>
                                     other; };
};

/**
 * STL compatible allocator that allocates/deallocates from thread local
 * memory pools that serve fixed size allocations
 */
template <typename T>
class FastAllocator {
public:
    typedef size_t    size_type;
    typedef ptrdiff_t difference_type;
    typedef T*        pointer;
    typedef const T*  const_pointer;
    typedef T&        reference;
    typedef const T&  const_reference;
    typedef T         value_type;
    template <typename U> struct rebind { typedef FastAllocator<U>
                                        other; };
    FastAllocator() {}

    template <typename U>
    FastAllocator( const FastAllocator<U> &other) {}
    FastAllocator(const FastAllocator<T> &other) {}

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

    bool operator==(const FastAllocator &other) const {
        return true;
    }

    bool operator!=(const FastAllocator &other) const {
        return false;
    }

    pointer allocate(const size_type n) {
        if (n == 0) {
            return NULL;
        }
        const pointer ret = (n == 1) ?
                static_cast<pointer>(
                        ThreadLocalPool::allocateExactSizedObject(sizeof(T))) :
                        reinterpret_cast<pointer>(new char[sizeof(T) * n]);
        if (ret == 0) {
            boost::throw_exception(std::bad_alloc());
        }
        return ret;
    }

    pointer allocate(const size_type n, const void * const) {
        return allocate(n);
    }

    pointer allocate() {
        const pointer ret = ThreadLocalPool::allocateExactSizedObject(sizeof(T));
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
            ThreadLocalPool::freeExactSizedObject(sizeof(T), ptr);
        }
        else {
            delete [] reinterpret_cast<const char*>(ptr);
        }
    }

    void deallocate(const pointer ptr) {
        if (ptr == NULL) {
            return;
        }
        ThreadLocalPool::freeExactSizedObject(sizeof(T), ptr);
    }
};
}
#endif /* FASTALLOCATOR_HPP_ */
