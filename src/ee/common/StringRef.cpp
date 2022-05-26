/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#include "storage/LargeTempTableBlock.h"

using namespace voltdb;

inline ThreadLocalPool::Sized* asSizedObject(char* stringPtr) {
   return reinterpret_cast<ThreadLocalPool::Sized*>(stringPtr);
}

char* StringRef::getObjectValue() {
   return asSizedObject(m_stringPtr)->m_data;
}

const char* StringRef::getObjectValue() const {
   return asSizedObject(m_stringPtr)->m_data;
}

int32_t StringRef::getObjectLength() const {
   return asSizedObject(m_stringPtr)->m_size;
}

const char* StringRef::getObject(int32_t& lengthOut) const {
    /*/ enable to debug
      std::cout << this << " DEBUG: getting [" << asSizedObject(m_stringPtr)->m_size << "]"
      << std::string(asSizedObject(m_stringPtr)->m_data,
      asSizedObject(m_stringPtr)->m_size)
      << std::endl;
    // */
    auto const* sized = asSizedObject(m_stringPtr);
    lengthOut = sized->m_size;
    vassert(lengthOut >= 0);
    return sized->m_data;
}

int32_t StringRef::getAllocatedSizeInPersistentStorage() const {
    // The CompactingPool allocated a chunk of this size for storage.
    int32_t alloc_size = ThreadLocalPool::getAllocationSizeForRelocatable(asSizedObject(m_stringPtr));
    //cout << "Pool allocation size: " << alloc_size << endl;
    // One of these was allocated in the thread local pool for the string
    alloc_size += static_cast<int32_t>(sizeof(StringRef));
    //cout << "StringRef size: " << sizeof(StringRef) << endl;
    //cout << "Total allocation size: " << alloc_size << endl;
    return alloc_size;
}

int32_t StringRef::getAllocatedSizeInTempStorage() const {
    int32_t size = asSizedObject(m_stringPtr)->m_size;
    return size + sizeof(StringRef) + sizeof(ThreadLocalPool::Sized);
}

// Persistent strings are initialized to point to relocatable storage.
// Deletions of OTHER strings can cause a compaction of the pool which
// has the effect of relocating that storage.
// Here, m_stringPtr gets initialized to the object's initial location,
// but, equally importantly, m_stringPtr's address is also passed to the
// allocator as a pointer to a freely MUTABLE pointer. This allows the
// allocator AT SOME FUTURE POINT to relocate the storage but keep this
// StringRef's cached pointer to that storage up to date.
// This purposely by-passes the private member protections that would
// make m_stringPtr immutable/invisible outside the StringRef class.
// Since this resetting of m_stringPtr happens synchronously on the thread
// that owns this StringRef and we never copy/cache m_stringPtr values,
// it's perfectly safe for the member to be changed this way.
// The alternative would be to require the allocator to be aware of the
// StringRef class so that it can call a proper accessor method.
// An earlier implementation taking this approach proved hard to follow.
inline StringRef::StringRef(int32_t sz):
    m_stringPtr(reinterpret_cast<char*>(ThreadLocalPool::allocateRelocatable(&m_stringPtr, sz))) { }

// Temporary strings are allocated in one piece with their referring
// StringRefs -- the string data starts just past the StringRef object,
// which by the rules of object pointer math is just "this+1".
inline StringRef::StringRef(Pool* unused, int32_t sz) : m_stringPtr(reinterpret_cast<char*>(this+1)) {
    asSizedObject(m_stringPtr)->m_size = sz;
}

// The destroy method keeps this from getting run on temporary strings.
inline StringRef::~StringRef() {
    ThreadLocalPool::freeRelocatable(asSizedObject(m_stringPtr));
}

StringRef* StringRef::create(int32_t sz, const char* source, Pool* tempPool) {
    /*/ enable to debug
    if (source) {
        std::cout << "DEBUG: setting [" << sz << "]" << std::string(source, sz) << std::endl;
    }
    else {
        std::cout << "DEBUG: setting up [" << sz << "]" << std::endl;
    }
    // */
    vassert(sz >= 0);
    StringRef* result;
    if (tempPool) {
        result = new (tempPool->allocate(sizeof(StringRef)+sizeof(ThreadLocalPool::Sized) + sz)) StringRef(tempPool, sz);
    } else {
#ifdef MEMCHECK
        result = new StringRef(sz);
#else
        result = new (ThreadLocalPool::allocateExactSizedObject(sizeof(StringRef))) StringRef(sz);
#endif
    }
    if (source && sz > 0) {
        ::memcpy(result->getObjectValue(), source, sz);
    }
    return result;
}

StringRef* StringRef::create(int32_t sz, const char* source, LargeTempTableBlock* lttBlock) {
    vassert(lttBlock != nullptr);
    StringRef* result;
    result = new (lttBlock->allocate(sizeof(StringRef)+sizeof(ThreadLocalPool::Sized) + sz)) StringRef(NULL, sz);

    if (source) {
        ::memcpy(result->getObjectValue(), source, sz);
    }
    return result;

}

void StringRef::relocate(std::ptrdiff_t offset) {
    m_stringPtr += offset;
}

// The destroy method keeps this from getting run on temporary strings.
void StringRef::operator delete(void* sref) {
#ifdef MEMCHECK
    ::operator delete(sref);
#else
    ThreadLocalPool::freeExactSizedObject(sizeof(StringRef), sref);
#endif
}

void StringRef::destroy(StringRef* sref) {
    // Temporary strings are allocated in one piece with their referring
    // StringRefs -- both get deallocated as raw storage when the temp pool
    // is purged or destroyed. They MUST NOT be deallocated here and now.
    // Pointer math provides an easy way (sref+1) to calculate the address
    // contiguous to the end of the StringRef object.
    // Persistent strings can never pass this test because they
    // set m_stringPtr only to an address that is at some offset into an
    // allocation that is separate from the StringRef. Even in the
    // unlikely event that the two allocations were very close to each other,
    // they would still be separated by that offset and would fail this
    // test.
    if (sref->m_stringPtr == reinterpret_cast<char*>(sref+1)) {
        return;
    }
    delete sref;
}
