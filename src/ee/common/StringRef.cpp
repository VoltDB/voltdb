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

#include "StringRef.h"

#include "Pool.hpp"
#include "ThreadLocalPool.h"

namespace voltdb
{

std::size_t StringRef::computeStringMemoryUsed(size_t length)
{
    // CompactingStringPool will allocate a chunk of this size for storage.
    // This size is the actual length plus the 4-byte length storage
    // plus the backpointer to the StringRef
    size_t alloc_size =
        ThreadLocalPool::getAllocationSizeForObject(length +
                                                    sizeof(int32_t) +
                                                    sizeof(StringRef*));
    //cout << "Object length: " << length << endl;
    //cout << "StringRef* size: " << sizeof(StringRef*) << endl;
    //cout << "Pool allocation size: " << alloc_size << endl;
    // One of these will be allocated in the thread local pool for the string
    alloc_size += sizeof(StringRef);
    //cout << "StringRef size: " << sizeof(StringRef) << endl;
    //cout << "Total allocation size: " << alloc_size << endl;
    return alloc_size;
}

StringRef* StringRef::create(size_t size, Pool* tempPool)
{
    if (tempPool) {
        return new (tempPool->allocate(sizeof(StringRef) + size)) StringRef();
    }
    return new (ThreadLocalPool::allocateExactSize(sizeof(StringRef))) StringRef(size);
}

void StringRef::operator delete(void* victim)
{ ThreadLocalPool::freeExactSizeAllocation(sizeof(StringRef), victim); }

StringRef::StringRef(size_t size)
  : m_size(size + sizeof(StringRef*))
  , m_stringPtr(ThreadLocalPool::allocateString(m_size))
{
    // Sanity check that this persistent string instance will not
    // be misidentified as a temp string.
    assert(!wasTempPoolAllocated());
    // Enable relocation of the referent string when its pool is compacted.
    char*** backptr = reinterpret_cast<char***>(m_stringPtr);
    *backptr = &m_stringPtr;
}

StringRef::~StringRef()
{ ThreadLocalPool::freeStringAllocation(m_size, m_stringPtr); }

void StringRef::destroy(StringRef* sref)
{
    // Temp Pool StringRefs and their referents just leak into the Pool until it is purged.
    // By-pass the destructor to avoid trying to free the referent which is not even a
    // separate allocation.
    if (sref->wasTempPoolAllocated()) {
        return;
    }
    delete sref;
}

}
