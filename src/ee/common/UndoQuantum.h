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

#pragma once

#include <vector>
#include <stdint.h>
#include <cassert>
#include <string.h>

#include "common/Pool.hpp"
#include "boost/unordered_set.hpp"
#include "UndoReleaseAction.h"

class StreamedTableTest;

namespace voltdb {
class UndoReleaseAction;
class UndoQuantumReleaseInterest;
class UndoLog;
class UndoQuantum {
    // UndoQuantum has a very limited public API that allows UndoAction registration
    // and copying buffers into pooled storage. Anything else is reserved for friends.
    friend class UndoLog; // For management access -- allocation, deallocation, etc.
    friend class UndoReleaseAction; // For allocateAction.
    friend class ::StreamedTableTest;

    const int64_t m_undoToken;
    std::vector<std::unique_ptr<UndoReleaseAction>> m_undoActions;
    uint32_t m_numInterests = 0;
    uint32_t m_interestsCapacity = 0;
    UndoQuantumReleaseInterest **m_interests = nullptr;
    const bool m_forLowestSite;

public:
    virtual void registerUndoAction(std::unique_ptr<UndoReleaseAction>&& undoAction, UndoQuantumReleaseInterest *interest = nullptr);
    int64_t getUndoToken() const {
        return m_undoToken;
    }

    int64_t getAllocatedMemory() const {
        return m_dataPool->getAllocatedMemory();
    }

    template <typename T> T allocatePooledCopy(T original, std::size_t sz) {
        return reinterpret_cast<T>(::memcpy(m_dataPool->allocate(sz), original, sz));
    }

    void* allocateAction(size_t sz) {
       return m_dataPool->allocate(sz);
    }
protected:
    Pool *m_dataPool;
    void* operator new(size_t sz, Pool& pool) { return pool.allocate(sz); }
    void operator delete(void*, Pool&) { /* emergency deallocator does nothing */ }
    void operator delete(void*) { /* every-day deallocator does nothing -- lets the pool cope */ }

    UndoQuantum(int64_t undoToken, Pool *dataPool, bool forLowestSite)
        : m_undoToken(undoToken), m_forLowestSite(forLowestSite), m_dataPool(dataPool) {}
    virtual ~UndoQuantum() {}
    /*
     * Invoke all the undo actions for this UndoQuantum. UndoActions
     * must have released all memory after undo() is called.
     * "delete" here only really calls their virtual destructors (important!)
     * but their no-op delete operator leaves them to be purged in one go with the data pool.
     */
    Pool* undo();

    /*
     * Call "release" and the destructors on all the UndoActions for this
     * UndoQuantum so they will release any resources they still hold.
     * "delete" here only really calls their virtual destructors (important!)
     * but their no-op delete operator leaves them to be purged in one go with the data pool.
     * Also call own destructor to ensure that the vector is released.
     *
     * The order of releasing should be FIFO order, which is the reverse of what
     * undo does. Think about the case where you insert and delete a bunch of
     * tuples in a table, then does a truncate. You do not want to delete that
     * table before all the inserts and deletes are released.
     */
    Pool* release();
};

}

