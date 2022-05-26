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

#pragma once

#include <cstddef>
#include <stdint.h>

namespace voltdb {
class Pool;
class LargeTempTableBlock;

/// An object to use in lieu of raw char* pointers for strings
/// which are not inlined into tuple storage.  This provides a
/// constant pointer value to be stored in tuple storage while
/// allowing the memory containing the actual string to be moved
/// around as the result of compaction.
class StringRef {
    // Signature used internally for persistent strings
    StringRef(int32_t size);
    // Signature used internally for temporary strings
    StringRef(Pool* tempPool, int32_t size);
    // Only called from destroy and only for persistent strings.
    ~StringRef();

    // Only called from destroy and only for persistent strings.
    void operator delete(void* object);

    char* m_stringPtr;
public:
    /// Utility method to extract the amount of memory that was
    /// used by non-inline storage for this string/varbinary.
    /// Includes the size of the pooled StringRef object,
    /// backpointer, and excess memory allocated in the compacting
    /// string pool.
    int32_t getAllocatedSizeInPersistentStorage() const;

    /// This method is just like getAllocatedSizeInPersistentStorage()
    /// but it returns the amount of memory required to store this
    /// object in temporary memory, including the overhead of the
    /// StringRef object and the length prefix of the data.
    int32_t getAllocatedSizeInTempStorage() const;

    /// Create and return a new StringRef object which points to an
    /// allocated memory block of the requested size.  The caller
    /// may provide an optional Pool from which the memory (and
    /// the memory for the StringRef object itself) will be
    /// allocated, intended for temporary strings.  If no Pool
    /// object is provided, the StringRef and the string memory will be
    /// allocated out of the ThreadLocalPool's persistent storage.
    static StringRef* create(int32_t size, const char* bytes, Pool* tempPool);

    /// This method works very much like the one above that accepts a
    /// Pool, but instead uses the LargeTempTableBlock to do
    /// allocation.  LargeTempTableBlocks store tuple data and
    /// non-inlined data in the same chunk of memory.
    static StringRef* create(int32_t size, const char* bytes, LargeTempTableBlock* lttBlock);

    /// Destroy the given StringRef object and free any memory
    /// allocated from persistent pools to store the object.
    /// sref must have been allocated and returned by a call to
    /// StringRef::create().
    /// This is a no-op for strings created in a temporary Pool
    /// -- temporary pools pool their allocations
    /// until the pool itself is purged or destroyed.
    /// Currently, the StringRefs for persistent strings are permanently
    /// allocated into a memory pool which is reserved for future reuse
    /// specifically as persistent StringRef memory.
    static void destroy(StringRef* sref);

    char* getObjectValue();
    const char* getObjectValue() const;

    int32_t getObjectLength() const;

    const char* getObject(int32_t& lengthOut) const;

    /// When a string is relocated, we need to update the data pointer.
    void relocate(std::ptrdiff_t offset);
};

} // namespace voltdb

