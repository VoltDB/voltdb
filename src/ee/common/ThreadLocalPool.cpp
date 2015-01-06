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
#include "common/ThreadLocalPool.h"
#include <pthread.h>
#include <boost/unordered_map.hpp>
#include "common/FatalException.hpp"
#include <iostream>
#include "common/SQLException.h"

// This needs to be >= the VoltType.MAX_VALUE_LENGTH defined in java, currently 1048576.
// The rationale for making it any larger would be to allow calculating wider "temp" values
// for use in situations where they are not being stored as column values.
static const int POOLED_MAX_VALUE_LENGTH = 1048576;

namespace voltdb {
/**
 * Thread local key for storing thread specific memory pools
 */
static pthread_key_t m_key;
static pthread_key_t m_stringKey;
/**
 * Thread local key for storing integer value of amount of memory allocated
 */
static pthread_key_t m_keyAllocated;
static pthread_once_t m_keyOnce = PTHREAD_ONCE_INIT;

typedef boost::unordered_map< std::size_t, boost::shared_ptr<boost::pool<voltdb_pool_allocator_new_delete> > > MapType;
typedef MapType* MapTypePtr;
typedef std::pair<int, MapTypePtr > PairType;
typedef PairType* PairTypePtr;

static void createThreadLocalKey() {
    (void)pthread_key_create( &m_key, NULL);
    (void)pthread_key_create( &m_stringKey, NULL);
    (void)pthread_key_create( &m_keyAllocated, NULL);
}

ThreadLocalPool::ThreadLocalPool() {
    (void)pthread_once(&m_keyOnce, createThreadLocalKey);
    if (pthread_getspecific(m_key) == NULL) {
        pthread_setspecific( m_keyAllocated, static_cast<const void *>(new std::size_t(0)));
        pthread_setspecific( m_key, static_cast<const void *>(
                new PairType(
                        1, new MapType())));
        pthread_setspecific(m_stringKey, static_cast<const void*>(new CompactingStringStorage()));
    } else {
        PairTypePtr p =
                static_cast<PairTypePtr>(pthread_getspecific(m_key));
        pthread_setspecific( m_key, new PairType( p->first + 1, p->second));
        delete p;
    }
}

ThreadLocalPool::~ThreadLocalPool() {
    PairTypePtr p =
            static_cast<PairTypePtr>(pthread_getspecific(m_key));
    assert(p != NULL);
    if (p != NULL) {
        if (p->first == 1) {
            delete p->second;
            pthread_setspecific( m_key, NULL);
            delete static_cast<CompactingStringStorage*>(pthread_getspecific(m_stringKey));
            pthread_setspecific(m_stringKey, NULL);
            delete static_cast<std::size_t*>(pthread_getspecific(m_keyAllocated));
            pthread_setspecific( m_keyAllocated, NULL);
        } else {
            pthread_setspecific( m_key, new PairType( p->first - 1, p->second));
        }
        delete p;
    }
}

std::size_t
ThreadLocalPool::getAllocationSizeForObject(std::size_t length) {
    if (length <= 2) {
        return 2;
    } else if (length <= 4) {
        return 4;
    } else if (length <= 4 + 2) {
        return 4 + 2;
    } else if (length <= 8) {
        return 8;
    } else if (length <= 8 + 4) {
        return 8 + 4;
    } else if (length <= 16) {
        return 16;
    } else if (length <= 16 + 8) {
        return 16 + 8;
    } else if (length <= 32) {
        return 32;
    } else if (length <= 32 + 16) {
        return 32 + 16;
    } else if (length <= 64) {
        return 64;
    } else if (length <= 64 + 32) {
        return 64 + 32;
    } else if (length <= 128) {
        return 128;
    } else if (length < 128 + 64) {
        return 128 + 64;
    } else if (length <= 256) {
        return 256;
    } else if (length <= 256 + 128) {
        return 256 + 128;
    } else if (length <= 512) {
        return 512;
    } else if (length <= 512 + 256) {
        return 512 + 256;
    } else if (length <= 1024) {
        return 1024;
    } else if (length <= 1024 + 512) {
        return 1024 + 512;
    } else if (length <= 2048) {
        return 2048;
    } else if (length <= 2048 + 1024) {
        return 2048 + 1024;
    } else if (length <= 4096) {
        return 4096;
    } else if (length < 4096 + 2048) {
        return 4096 + 2048;
    } else if (length <= 8192) {
        return 8192;
    } else if (length < 8192 + 4096) {
        return 8192 + 4096;
    } else if (length <= 16384) {
        return 16384;
    } else if (length <= 16384 + 8192) {
        return 16384 + 8192;
    } else if (length <= 32768) {
        return 32768;
    } else if (length <= 32768 + 16384) {
        return 32768 + 16384;
    } else if (length <= 65536) {
        return 65536;
    } else if (length <= 65536 + 32768) {
        return 65536 + 32768;
    } else if (length <= 131072) {
        return 131072;
    } else if (length <= 131072 + 65536) {
        return 131072 + 65536;
    } else if (length <= 262144) {
        return 262144;
    } else if (length <= 262144 + 131072) {
        return 262144 + 131072;
    } else if (length <= 524288) {
        return 524288;
    } else if (length <= 524288 + 262144) {
        return 524288 + 262144;
        //Need space for a length prefix and a backpointer
    } else if (length <= POOLED_MAX_VALUE_LENGTH + sizeof(int32_t) + sizeof(void*)) {
        return POOLED_MAX_VALUE_LENGTH + sizeof(int32_t) + sizeof(void*);
    } else {
        // Do this so that we can use this method to compute allocation sizes.
        // Expect callers to check for 0 and throw a FatalException for
        // illegal size.
        return 0;
    }
}

CompactingStringStorage*
ThreadLocalPool::getStringPool()
{
    return static_cast<CompactingStringStorage*>(pthread_getspecific(m_stringKey));
}

boost::shared_ptr<boost::pool<voltdb_pool_allocator_new_delete> > ThreadLocalPool::get(std::size_t size) {
    size_t alloc_size = getAllocationSizeForObject(size);
    if (alloc_size == 0)
    {
        throwDynamicSQLException("Attempted to allocate an object > than the 1 meg limit. Requested size was %du",
            static_cast<int32_t>(size));
    }
    return getExact(alloc_size);
}

boost::shared_ptr<boost::pool<voltdb_pool_allocator_new_delete> > ThreadLocalPool::getExact(std::size_t size) {
    MapTypePtr pools =
            static_cast< PairTypePtr >(pthread_getspecific(m_key))->second;
    boost::unordered_map< std::size_t, boost::shared_ptr<boost::pool<voltdb_pool_allocator_new_delete> > >::iterator
        iter = pools->find(size);
    if (iter == pools->end()) {
        boost::shared_ptr<boost::pool<voltdb_pool_allocator_new_delete> > pool =
                boost::shared_ptr<boost::pool<voltdb_pool_allocator_new_delete> >(
                        new boost::pool<voltdb_pool_allocator_new_delete>(size));
        pools->insert( std::pair<std::size_t, boost::shared_ptr<boost::pool<voltdb_pool_allocator_new_delete> > >(size, pool));
        return pool;
    }
    boost::shared_ptr<boost::pool<voltdb_pool_allocator_new_delete> > pool = iter->second;

    /**
     * The goal of this code is to bypass the pool sizing algorithm used by boost
     * and replace it with something that bounds allocations to a series of 2 meg blocks
     * for small allocations. For large allocations fall back to a strategy of allocating two of
     * these huge things at a time. The goal of this bounding is make the amount of untouched but allocated
     * memory relatively small so that the counting done by the volt allocator accurately represents the effect
     * on RSS.
     */
    if (pool->get_next_size() * pool->get_requested_size() > (1024 * 1024 * 2)) {
        //If the size of objects served by this pool is less than 256 kilobytes
        //go ahead an allocated a 2 meg block of them
        if (pool->get_requested_size() < (1024 * 256)) {
            pool->set_next_size((1024 * 1024 * 2) /  pool->get_requested_size());
        } else {
            //For large objects allocated just two of them
            pool->set_next_size(2);
        }
    }
    return iter->second;
}

std::size_t ThreadLocalPool::getPoolAllocationSize() {
    size_t bytes_allocated =
        *static_cast< std::size_t* >(pthread_getspecific(m_keyAllocated));
    bytes_allocated += (static_cast<CompactingStringStorage*>(pthread_getspecific(m_stringKey)))->getPoolAllocationSize();
    return bytes_allocated;
}

char * voltdb_pool_allocator_new_delete::malloc(const size_type bytes) {
    (*static_cast< std::size_t* >(pthread_getspecific(m_keyAllocated))) += bytes + sizeof(std::size_t);
    //std::cout << "Pooled memory is " << ((*static_cast< std::size_t* >(pthread_getspecific(m_keyAllocated))) / (1024 * 1024)) << " after requested allocation " << (bytes / (1024 * 1024)) <<  std::endl;
    char *retval = new (std::nothrow) char[bytes + sizeof(std::size_t)];
    *reinterpret_cast<std::size_t*>(retval) = bytes + sizeof(std::size_t);
    return &retval[sizeof(std::size_t)];
}

void voltdb_pool_allocator_new_delete::free(char * const block) {
    (*static_cast< std::size_t* >(pthread_getspecific(m_keyAllocated))) -= *reinterpret_cast<std::size_t*>(block - sizeof(std::size_t));
    delete [](block - sizeof(std::size_t));
}
}
