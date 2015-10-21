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

#ifndef STRINGREF_H
#define STRINGREF_H

#include <cstddef>

namespace voltdb
{
    class Pool;

    /// An object to use in lieu of raw char* pointers for strings
    /// which are not inlined into tuple storage.  This provides a
    /// constant value to live in tuple storage while allowing the memory
    /// containing the actual string to be moved around as the result of
    /// compaction.
    class StringRef
    {
    public:
        /// Utility method to compute the amount of memory that will
        /// be used by non-inline storage of a string/varbinary of the
        /// given length.  Includes the size of pooled StringRef object,
        /// backpointer, and excess memory allocated in the compacting
        /// string pool.
        static std::size_t computeStringMemoryUsed(std::size_t length);

        friend class CompactingStringPool;
        /// Create and return a new StringRef object which points to an
        /// allocated memory block of the requested size.  The caller
        /// may provide an optional Pool from which the memory (and
        /// the memory for the StringRef object itself) will be
        /// allocated, intended for temporary strings.  If no Pool
        /// object is provided, the StringRef and the string memory will be
        /// allocated out of the ThreadLocalPool.
        static StringRef* create(std::size_t size, Pool* dataPool);

        /// Destroy the given StringRef object and free any memory
        /// allocated from persistet pools to store the object.
        /// sref must have been allocated and returned by a call to
        /// StringRef::create() and is a no-op for strings created in
        /// a temporary Pool -- they simply leak their allocations
        /// until the Pool is purged or destroyed.
        static void destroy(StringRef* sref);

        char* get();
        const char* get() const;

    private:
        StringRef(std::size_t size);
        StringRef(std::size_t size, Pool* dataPool);
        // Only called from destroy and only for persistent strings.
        ~StringRef();

        // Only called from destroy and only for persistent strings.
        void operator delete(void* object);

        /// Callback used via the back-pointer in order to update the
        /// pointer to the memory backing this string reference
        void updateStringLocation(void* location);

        void setBackPtr();

        std::size_t m_size;
        bool m_tempPool;
        char* m_stringPtr;
    };
}

#endif // STRINGREF_H
