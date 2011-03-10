/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
#include "common/CompactingStringStorage.h"

#include "common/FatalException.hpp"
#include <boost/unordered_map.hpp>
#include <iostream>

using namespace voltdb;
using namespace std;
using boost::shared_ptr;

namespace
{
    size_t getAllocationSizeForObject(size_t length)
    {
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
        } else if (length <= 1048576 + sizeof(int32_t) + sizeof(void*)) {
            return 1048576 + sizeof(int32_t) + sizeof(void*);
        } else {
            throwFatalException("Attempted to allocate an object then the 1 meg limit. Requested size was %Zu", length);
        }
        // NOT REACHED
        return length + 4;
    }
}

typedef boost::shared_ptr<CompactingStringPool> PoolPtrType;
typedef boost::unordered_map<size_t, PoolPtrType> MapType;

CompactingStringStorage::CompactingStringStorage()
{
}

CompactingStringStorage::~CompactingStringStorage()
{
}

PoolPtrType
CompactingStringStorage::get(size_t size)
{
    size = getAllocationSizeForObject(size);
    return getExact(size);
}

PoolPtrType
CompactingStringStorage::getExact(size_t size)
{
    MapType::iterator iter = m_poolMap.find(size);
    int32_t ssize = static_cast<int32_t>(size);
    PoolPtrType pool;
    if (iter == m_poolMap.end()) {
        // compute num_elements to be closest multiple
        // leading to a 2Meg buffer
        int32_t num_elements = (2 * 1024 * 1024 / ssize) + 1;
        pool = PoolPtrType(new CompactingStringPool(ssize, num_elements));
        m_poolMap.insert(pair<size_t, PoolPtrType>(size, pool));
    }
    else
    {
        pool = iter->second;
    }
    return pool;
}

size_t CompactingStringStorage::getPoolAllocationSize()
{
    size_t total = 0;
    for (MapType::iterator iter = m_poolMap.begin();
         iter != m_poolMap.end();
         ++iter)
    {
        total += iter->second->getBytesAllocated();
    }
    return total;
}
