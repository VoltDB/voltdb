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

#include "StringRef.h"

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
    if (mutated)
    {
        // use the backpointer to the StringRef object in the moved
        // data to update that object with the new string location
        StringRef* back_ptr = *reinterpret_cast<StringRef**>(element);
        back_ptr->updateStringLocation(element);
    }
}

size_t
CompactingStringPool::getBytesAllocated() const
{
    return m_pool.getBytesAllocated();
}
