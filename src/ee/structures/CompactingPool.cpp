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

#include "CompactingPool.h"

#include <cstring>

using namespace voltdb;
using namespace std;

CompactingPool::CompactingPool(int32_t elementSize, int32_t elementsPerBuffer) :
    m_size(elementSize), m_allocator(elementSize, elementsPerBuffer)
{
}

void*
CompactingPool::malloc()
{
    return m_allocator.alloc();
}

bool
CompactingPool::free(void* element)
{
    bool element_moved = false;
    void* last = m_allocator.last();
    if (last != element)
    {
        // copy the last element into the newly vacated spot
        memcpy(element, last, m_size);
        element_moved = true;
    }
    m_allocator.trim();
    return element_moved;
}

size_t
CompactingPool::getBytesAllocated() const
{
    return m_allocator.bytesAllocated();
}
