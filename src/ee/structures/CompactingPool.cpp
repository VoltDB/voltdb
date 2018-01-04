/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

#include "common/ThreadLocalPool.h"
#include "boost/foreach.hpp"

#include <cassert>

namespace voltdb
{

#ifdef VOLT_DEBUG_ENABLED
CompactingPool::~CompactingPool() {
    if (!m_allocations.empty()) {
        VOLT_ERROR("ContiguousAllocator data not deallocated on thread for partition %d",
                ThreadLocalPool::getThreadPartitionId());
        VOLT_ERROR_STACK();
        assert(false);
    }
#ifdef VOLT_TRACE_ALLOCATIONS
    BOOST_FOREACH (AllocTraceMap_t::value_type& entry, m_allocations) {
        VOLT_ERROR("Missing deallocation for %p at:", entry.first);
        entry.second->printLocalTrace();
        delete entry.second;
    }
#else
    BOOST_FOREACH (void* entry, m_allocations) {
        VOLT_ERROR("Missing deallocation for %p at:", entry);
    }
#endif
    m_allocations.clear();
}

void CompactingPool::setPtr(void* data) {
    VOLT_TRACE("ContiguousAllocator allocated %p", data);
#ifdef VOLT_TRACE_ALLOCATIONS
    StackTrace* st = new StackTrace();
    bool success = m_allocations.emplace(data, st).second;
#else
    bool success = m_allocations.emplace(data).second;
#endif
    if (!success) {
        VOLT_ERROR("ContiguousAllocator previously allocated (see below) pointer %p is being allocated"
                " a second time on thread (partition %d)", data, ThreadLocalPool::getEnginePartitionId());
#ifdef VOLT_TRACE_ALLOCATIONS
        m_allocations[data]->printLocalTrace();
        delete st;
#endif
        assert(false);
    }
}

void CompactingPool::movePtr(void* oldData, void* newData) {
    VOLT_TRACE("ContiguousAllocator Moved %p to %p", oldData, newData);
    AllocTraceMap_t::const_iterator it = m_allocations.find(oldData);
    if (it == m_allocations.end()) {
        VOLT_TRACE("ContiguousAllocator deallocated data pointer %p in wrong context thread (partition %d)",
                oldData, ThreadLocalPool::getEnginePartitionId());
        VOLT_ERROR_STACK();
        assert(false);
    }
    else {
#ifdef VOLT_TRACE_ALLOCATIONS
        bool success = m_allocations.emplace(newData, it->second).second;
#else
        bool success = m_allocations.emplace(newData).second;
#endif
        if (!success) {
            VOLT_ERROR("ContiguousAllocator previously allocated (see below) pointer %p is being allocated"
                    " a second time on thread (partition %d)", newData, ThreadLocalPool::getEnginePartitionId());
#ifdef VOLT_TRACE_ALLOCATIONS
            m_allocations[newData]->printLocalTrace();
            delete it->second;
#endif
            assert(false);
        }
        m_allocations.erase(it);
    }


}

bool CompactingPool::clrPtr(void* data) {
    VOLT_TRACE("Deallocated %p", data);
    AllocTraceMap_t::const_iterator it = m_allocations.find(data);
    if (it == m_allocations.end()) {
        VOLT_ERROR("Deallocated data pointer %p in wrong context thread (partition %d)", data, ThreadLocalPool::getEnginePartitionId());
        VOLT_ERROR_STACK();
        return false;
    }
    else {
#ifdef VOLT_TRACE_ALLOCATIONS
        delete it->second;
#endif
        m_allocations.erase(it);
    }
    return true;
}
#endif

} // namespace voltdb
