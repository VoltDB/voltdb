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
#include "CompactingStringStorage.h"

using namespace voltdb;
using namespace std;

size_t
StringRef::computeStringMemoryUsed(size_t length)
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

StringRef*
StringRef::create(size_t size, Pool* tempPool)
{
    if (tempPool != NULL) {
        return new (tempPool->allocate(sizeof(StringRef))) StringRef(size, tempPool);
    }
#ifdef MEMCHECK
    return new StringRef(size);
#else
    return new (ThreadLocalPool::allocateExactSizedObject(sizeof(StringRef))) StringRef(size);
#endif
}

void StringRef::operator delete(void* sref)
{
#ifdef MEMCHECK
    ::operator delete(sref);
#else
    ThreadLocalPool::freeExactSizedObject(sizeof(StringRef), sref);
#endif
}

void
StringRef::destroy(StringRef* sref)
{
    if (sref->m_tempPool) {
        return;
    }
    delete sref;
}

StringRef::StringRef(size_t size)
{
    m_size = size + sizeof(StringRef*);
    m_tempPool = false;
    m_stringPtr = ThreadLocalPool::allocateRelocatable(m_size);
    setBackPtr();
}

StringRef::StringRef(std::size_t size, Pool* tempPool)
{
    m_tempPool = true;
    m_stringPtr =
        reinterpret_cast<char*>(tempPool->allocate(size + sizeof(StringRef*)));
    setBackPtr();
}

StringRef::~StringRef()
{
    ThreadLocalPool::freeRelocatable(m_size, m_stringPtr);
}

char*
StringRef::get()
{
    return m_stringPtr + sizeof(StringRef*);
}

const char*
StringRef::get() const
{
    return m_stringPtr + sizeof(StringRef*);
}

void
StringRef::updateStringLocation(void* location)
{
    m_stringPtr = reinterpret_cast<char*>(location);
}

void
StringRef::setBackPtr()
{
    StringRef** backptr = reinterpret_cast<StringRef**>(m_stringPtr);
    *backptr = this;
}
