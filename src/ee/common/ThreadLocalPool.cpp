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
#include "common/ThreadLocalPool.h"
#include <pthread.h>
#include <boost/unordered_map.hpp>
#include "common/FatalException.hpp"

namespace voltdb {
/**
 * Thread local key for storing thread specific memory pools
 */
static pthread_key_t m_key;
static pthread_once_t m_keyOnce = PTHREAD_ONCE_INIT;

typedef boost::unordered_map< std::size_t, boost::shared_ptr<boost::pool<boost::default_user_allocator_new_delete> > > MapType;
typedef MapType* MapTypePtr;
typedef std::pair<int, MapTypePtr> PairType;
typedef std::pair<int, MapTypePtr>* PairTypePtr;

static void createThreadLocalKey() {
    (void)pthread_key_create( &m_key, NULL);
}

ThreadLocalPool::ThreadLocalPool() {
    (void)pthread_once(&m_keyOnce, createThreadLocalKey);
    if (pthread_getspecific(m_key) == NULL) {
        pthread_setspecific( m_key, static_cast<const void *>(
                new PairType(
                        1, new MapType())));
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
    if (p->first == 1) {
        delete p->second;
        pthread_setspecific( m_key, NULL);
    } else {
        pthread_setspecific( m_key, new PairType( p->first - 1, p->second));
    }
    delete p;
}

static std::size_t getAllocationSizeForObject(std::size_t length) {
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
        //Need space for a length prefix
    } else if (length <= 1048576 + 4) {
        return 1048576 + 4;
    } else {
        throwFatalException("Attempted to allocate an object then the 1 meg limit. Requested size was %Zu", length);
    }
    return length + 4;
}

boost::shared_ptr<boost::pool<boost::default_user_allocator_new_delete> > ThreadLocalPool::get(std::size_t size) {
    size = getAllocationSizeForObject(size);
    return getExact(size);
}

boost::shared_ptr<boost::pool<boost::default_user_allocator_new_delete> > ThreadLocalPool::getExact(std::size_t size) {
    MapTypePtr pools =
            static_cast< PairTypePtr >(pthread_getspecific(m_key))->second;
    boost::unordered_map< std::size_t, boost::shared_ptr<boost::pool<boost::default_user_allocator_new_delete> > >::iterator iter = pools->find(size);
    if (iter == pools->end()) {
        boost::shared_ptr<boost::pool<boost::default_user_allocator_new_delete> > pool = boost::shared_ptr<boost::pool<boost::default_user_allocator_new_delete> >(new boost::pool<boost::default_user_allocator_new_delete>(size));
        pools->insert( std::pair<std::size_t, boost::shared_ptr<boost::pool<boost::default_user_allocator_new_delete> > >(size, pool));
        return pool;
    }
    return iter->second;
}
}
