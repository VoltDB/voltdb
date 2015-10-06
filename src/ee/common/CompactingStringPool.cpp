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

#include "CompactingStringPool.h"

using namespace voltdb;
using namespace std;

CompactingStringPool::CompactingStringPool(int32_t elementSize,
                                           int32_t elementsPerBuf) :
    m_pool(elementSize, elementsPerBuf)
{
}

void*
CompactingStringPool::malloc()
{
    return m_pool.malloc();
}

void
CompactingStringPool::free(void* element)
{
    bool mutated = m_pool.free(element);
    if (mutated) {
        // Use the back pointer copied from the moved string
        // to locate the forward pointer to that string
        // and update it to point to the new string location
        void*** back_ptr = reinterpret_cast<void***>(element);
        void** forward_ptr = *back_ptr;
        *forward_ptr = element;
    }
}

size_t
CompactingStringPool::getBytesAllocated() const
{
    return m_pool.getBytesAllocated();
}
