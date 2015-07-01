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

#ifndef _EE_STRUCTURES_COMPACTINGPOOL_H_
#define _EE_STRUCTURES_COMPACTINGPOOL_H_

#include "ContiguousAllocator.h"

#include <cstdlib>

namespace voltdb
{
    // A semi-generic class to provide a compacting pool of objects of
    // fixed-size elementSize.  I think with some creative interface
    // definition and some templating this could be made into a more
    // generic pool that would be able to handle the backpointer
    // updating as well.
    class CompactingPool
    {
    public:
        // Create a compacting pool.  As memory is required, it will
        // allocate buffers of size elementSize * elementsPerBuffer bytes
        CompactingPool(int32_t elementSize, int32_t elementsPerBuffer);

        // get a pointer to elementSize bytes of free memory
        void* malloc();

        // Returns true if an element got compacted into this free'd space
        // element must be a pointer returned by malloc()
        bool free(void* element);

        // Return the number of bytes allocated for this pool.
        size_t getBytesAllocated() const;

    private:
        int32_t m_size;
        ContiguousAllocator m_allocator;
    };
}


#endif // _EE_STRUCTURES_COMPACTINGPOOL_H_
