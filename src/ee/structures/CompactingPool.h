/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#ifndef _EE_STRUCTURES_COMPACTINGPOOL_H_
#define _EE_STRUCTURES_COMPACTINGPOOL_H_

#include "ContiguousAllocator.h"

#include <cassert>
#include <cstring>
#include <boost/foreach.hpp>
#include <boost/unordered_set.hpp>

namespace voltdb {
// Provide a compacting pool of objects of fixed size. Each object is assumed
// to have a single char* pointer referencing it in the caller for the lifetime
// of the allocation.
// TODO: Strictly speaking, relocation could continue to work as long as there
// was only one referring pointer AT A GIVEN TIME. That would simply require
// that the CompactingPool allocation get notified of the "move"
// -- but this simple API is not yet implemented, so the referring pointer
// must remain at a fixed address for the lifetime of the allocation
// -- only the allocation itself can be moved.
// We may loosen this requirement in the future, if needed.
// Note, once the caller provides a "forward pointer" to the allocation, it
// gives up control of the forward pointer's value for the lifetime of the
// allocation. The caller must avoid copying or caching this pointer or
// exposing it to asynchronous uses on other threads.
// Whenever Compacting Pool code is passed control on the current thread,
// it reserves the right to relocate past allocations on the thread and reset
// their forward pointers to copied versions of their allocations.
// Currently, this only happens when (other) allocations are freed.
class CompactingPool
{
  public:
    // Create a compacting pool.  As memory is required, it will
    // allocate buffers of size elementSize * elementsPerBuffer bytes.
    CompactingPool(int32_t elementSize, int32_t elementsPerBuffer)
      : m_allocator(elementSize + FIXED_OVERHEAD_PER_ENTRY(), elementsPerBuffer)
      , m_allocationsPendingRelease()
    { }

    void* malloc(char** referrer)
    {
        Relocatable* result =
            Relocatable::fromAllocation(m_allocator.alloc(), referrer);
        // Going forward, the compacting pool manages the value of
        // *referrer -- "the pointer to the allocation",
        // but it's initial value is set by the caller BASED ON --
        // THAT IS, RELATIVE TO BUT NOT NECCESARILY EQUAL TO --
        // the value returned here.
        // This allows this allocator to be part of a layering of
        // allocators that each injects its own header information
        // into the allocation and returns an offset into the allocation
        // to the actual caller responsible for storing the pointer
        // value.
        // Future maintenance of the referrer only assumes that it must
        // point to an address at the same relative offset from the
        // actual allocation address before and after the allocation is
        // relocated.
        return result->m_data;
    }

    /**
     * Put this pointer to allocated data in a set of items
     * that will be freed when client calls freePendingAllocations().
     */
    void markAllocationAsPendingRelease(void* element)
    {
        m_allocationsPendingRelease.insert(element);
    }

    /**
     * Free all the allocations in the allocationsPendingRelease set.
     *
     * There is a performance advantage to freeing large numbers of
     * allocations at once because we can minimize the amount of
     * memove'ing we need to do---we can avoid moving allocations that
     * are about to be freed anyway.
     */
    void freePendingAllocations()
    {
        auto end = m_allocationsPendingRelease.end();
        decltype(end) it;
        do {
            if (trimAllocationsPendingRelease()) {
                // This function returns true if there are no
                // allocations pending release.
                break;
            }

            it = m_allocationsPendingRelease.begin();
            assert (it != end);

            auto toBeReleased = it;
            ++it;

            free(*toBeReleased);
            m_allocationsPendingRelease.erase(toBeReleased);
        }
        while (it != end);

        assert (m_allocationsPendingRelease.size() == 0);
    }

    std::size_t getBytesAllocated() const
    { return m_allocator.bytesAllocated(); }

    static int32_t FIXED_OVERHEAD_PER_ENTRY()
    { return static_cast<int32_t>(sizeof(Relocatable)); }

  private:

    /**
     * If the last() references an item that is pending release, then
     * trim it.  Do this until last() references an item that is not
     * pending release, or until there are no more allocations.
     *
     * Returns true if all allocations pending release have been freed.
     */
    bool trimAllocationsPendingRelease() {
        if (m_allocator.count() == 0)
            return true;

        Relocatable* last = reinterpret_cast<Relocatable*>(m_allocator.last());
        auto trimIt = m_allocationsPendingRelease.find(last->m_data);
        auto end = m_allocationsPendingRelease.end();
        while (trimIt != end) {
            m_allocator.trim();
            m_allocationsPendingRelease.erase(trimIt);

            if (m_allocator.count() == 0) {
                assert (m_allocationsPendingRelease.empty());
                return true;
            }

            last = reinterpret_cast<Relocatable*>(m_allocator.last());
            trimIt = m_allocationsPendingRelease.find(last->m_data);
        }

        return m_allocationsPendingRelease.empty();
    }

    void free(void* element)
    {
        Relocatable* vacated = Relocatable::backtrackFromCallerData(element);
        Relocatable* last = reinterpret_cast<Relocatable*>(m_allocator.last());
        if (last != vacated) {
            // Notify last's referrer that it is about to relocate
            // to the location vacated by element.
            // Use relative addresses to maintain the same byte offset between
            // *(last->m_referringPtr) and its referent.
            // This allows layered allocators to have injected their own header
            // structures between the start of the Relocatable's m_data and the
            // address that the top allocator returned to its caller to store
            // and use for its own data.
            *(last->m_referringPtr) += (vacated->m_data - last->m_data);
            // copy the last entry into the newly vacated spot
            memcpy(vacated, last, m_allocator.allocationSize());
        }
        // retire the last entry.
        m_allocator.trim();
    }

    ContiguousAllocator m_allocator;

    boost::unordered_set<void*> m_allocationsPendingRelease;

    /// The layout of a relocatable allocation,
    /// including overhead for managing the relocation process.
    /// The layout contains a back-pointer to the referring pointer that
    /// the caller provided for the allocation. The back-pointer is
    /// stored in a header that is completely invisible to the caller.
    /// The caller is exposed only to the remaining "data" part of the
    /// allocation, of at least their requested size.
    /// So, the address of this data part is the proper return value for
    /// malloc (initial location) and the proper target address to use
    /// for later updates to the referring pointer (the new location for
    /// the allocation).
    struct Relocatable {
        char** m_referringPtr;
        char m_data[0];

        static Relocatable* fromAllocation(void* allocation, char** referrer)
        {
            Relocatable* result = reinterpret_cast<Relocatable*>(allocation);
            result->m_referringPtr = referrer;
            return result;
        }

        static Relocatable* backtrackFromCallerData(void* data)
        {
            // "-1" for a Relocatable* subtracts sizeof(Relocatable) ==
            // sizeof(m_referringPtr) == 8 bytes.
            Relocatable* result = reinterpret_cast<Relocatable*>(data)-1;
            // the data addresses should line up perfectly.
            assert(data == result->m_data);
            return result;
        }
    };
};

} // namespace voltdb

#endif // _EE_STRUCTURES_COMPACTINGPOOL_H_
