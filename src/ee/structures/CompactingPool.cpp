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
#include "CompactingPool.h"

#include "common/FatalException.hpp"
#include "common/ThreadLocalPool.h"

namespace voltdb {

#ifdef VOLT_POOL_CHECKING
CompactingPool::~CompactingPool() {
    if (!m_shutdown && !m_allocations.empty()) {
        VOLT_ERROR("ContiguousAllocator data not deallocated on thread for partition %d",
                ThreadLocalPool::getThreadPartitionId());
        VOLT_ERROR_STACK();
#ifdef VOLT_TRACE_ALLOCATIONS
        for (auto const& entry: m_allocations) {
            VOLT_ERROR("Missing deallocation for %p at:", entry.first);
            entry.second->printLocalTrace();
            delete entry.second;
        }
#else
        for(void* entry: m_allocations) {
            VOLT_ERROR("Missing deallocation for %p at:", entry);
        }
#endif
        vassert(false);
    }
    m_allocations.clear();
}

void CompactingPool::setPtr(void* data) {
    VOLT_TRACE("ContiguousAllocator allocated %p in context thread (partition %d)", data, ThreadLocalPool::getEnginePartitionId());
#ifdef VOLT_TRACE_ALLOCATIONS
    StackTrace* st = new StackTrace();
    bool success = m_allocations.insert(std::make_pair(data, st)).second;
#else
    bool success = m_allocations.insert(data).second;
#endif
    if (!success) {
        VOLT_ERROR("ContiguousAllocator previously allocated (see below) pointer %p is being allocated"
                " a second time in context thread (partition %d)", data, ThreadLocalPool::getEnginePartitionId());
#ifdef VOLT_TRACE_ALLOCATIONS
        m_allocations[data]->printLocalTrace();
        delete st;
#endif
        throwFatalException("Previously allocated relocatable object mysteriously re-allocated");
    }
}

void CompactingPool::movePtr(void* oldData, void* newData) {
    VOLT_TRACE("ContiguousAllocator Moved %p to %p in context thread (partition %d)", oldData, newData, ThreadLocalPool::getEnginePartitionId());
    AllocTraceMap_t::const_iterator it = m_allocations.find(oldData);
    if (it == m_allocations.end()) {
        VOLT_TRACE("ContiguousAllocator deallocated data pointer %p in wrong context thread (partition %d)",
                oldData, ThreadLocalPool::getEnginePartitionId());
        VOLT_ERROR_STACK();
    } else {
#ifdef VOLT_TRACE_ALLOCATIONS
        bool success = m_allocations.insert(std::make_pair(newData, it->second)).second;
#else
        bool success = m_allocations.insert(newData).second;
#endif
        if (!success) {
            VOLT_ERROR("ContiguousAllocator previously allocated (see below) pointer %p is being allocated"
                    " a second time in context thread (partition %d)", newData, ThreadLocalPool::getEnginePartitionId());
#ifdef VOLT_TRACE_ALLOCATIONS
            m_allocations[newData]->printLocalTrace();
            delete it->second;
#endif
            throwFatalException("Previously allocated relocatable object mysteriously re-allocated during move");
        } else {
            m_allocations.erase(it);
        }
    }
}

bool CompactingPool::clrPtr(void* data) {
    VOLT_TRACE("Deallocated %p in context thread (partition %d)", data, ThreadLocalPool::getEnginePartitionId());
    AllocTraceMap_t::const_iterator it = m_allocations.find(data);
    if (it == m_allocations.end()) {
        VOLT_ERROR("Deallocated data pointer %p in wrong context thread (partition %d)", data, ThreadLocalPool::getEnginePartitionId());
        VOLT_ERROR_STACK();
        throwFatalException("Deallocation of unknown pointer to relocatable object");
    } else {
#ifdef VOLT_TRACE_ALLOCATIONS
        delete it->second;
#endif
        m_allocations.erase(it);
    }
    return true;
}
#endif

} // namespace voltdb
