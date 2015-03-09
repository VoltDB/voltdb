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
StringRef::create(size_t size, Pool* dataPool)
{
    StringRef* retval;
    if (dataPool != NULL)
    {
        retval =
            new(dataPool->allocate(sizeof(StringRef))) StringRef(size, dataPool);
    }
    else
    {
#ifdef MEMCHECK
        retval = new StringRef(size);
#else
        retval = new(ThreadLocalPool::get(sizeof(StringRef))->malloc()) StringRef(size);
#endif
    }
    return retval;
}

void
StringRef::destroy(StringRef* sref)
{
#ifdef MEMCHECK
    delete sref;
#else
    bool temp_pool = sref->m_tempPool;
    sref->~StringRef();
    if (!temp_pool)
    {
        ThreadLocalPool::get(sizeof(StringRef))->free(sref);
    }
#endif
}

StringRef::StringRef(size_t size)
{
    m_size = size + sizeof(StringRef*);
    m_tempPool = false;
#ifdef MEMCHECK
    m_stringPtr = new char[m_size];
#else
    m_stringPtr =
        reinterpret_cast<char*>(ThreadLocalPool::getStringPool()->get(m_size)->malloc());
#endif
    setBackPtr();
}

StringRef::StringRef(std::size_t size, Pool* dataPool)
{
    m_tempPool = true;
    m_stringPtr =
        reinterpret_cast<char*>(dataPool->allocate(size + sizeof(StringRef*)));
    setBackPtr();
}

StringRef::~StringRef()
{
    if (!m_tempPool)
    {
#ifdef MEMCHECK
        delete[] m_stringPtr;
#else
        ThreadLocalPool::getStringPool()->get(m_size)->free(m_stringPtr);
#endif
    }
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
