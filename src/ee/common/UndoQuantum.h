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

#ifndef UNDOQUANTUM_H_
#define UNDOQUANTUM_H_

#include <stdint.h>
#include <cassert>
#include <string.h>
#include <list>

#include "common/Pool.hpp"
#include "common/VoltContainer.hpp"
#include "common/UndoReleaseAction.h"
#include "common/UndoQuantumReleaseInterest.h"
#include "boost/unordered_set.hpp"

class StreamedTableTest;
class TableAndIndexTest;

namespace voltdb {
class UndoLog;


class UndoQuantum {
    // UndoQuantum has a very limited public API that allows UndoAction registration
    // and copying buffers into pooled storage. Anything else is reserved for friends.
    friend class UndoLog; // For management access -- allocation, deallocation, etc.
    friend class UndoReleaseAction; // For allocateAction.
    friend class ::StreamedTableTest;

protected:
    void* operator new(size_t sz, Pool& pool) { return pool.allocate(sz); }
    void operator delete(void*, Pool&) { /* emergency deallocator does nothing */ }
    void operator delete(void*) { /* every-day deallocator does nothing -- lets the pool cope */ }

    inline UndoQuantum(int64_t undoToken, Pool *dataPool, bool forLowestSite)
        : m_undoToken(undoToken), m_numInterests(0), m_interestsCapacity(0),
          m_forLowestSite(forLowestSite), m_dataPool(dataPool) {}
    inline virtual ~UndoQuantum() {}

public:
    virtual inline void registerUndoAction(UndoReleaseAction *undoAction, UndoQuantumReleaseInterest *interest = NULL) {
        assert(undoAction);
        m_undoActions.push_back(undoAction);

        if (interest != NULL) {
           m_interests.push_back(interest);
        }
    }

protected:
    /*
     * Invoke all the undo actions for this UndoQuantum. UndoActions
     * must have released all memory after undo() is called.
     * "delete" here only really calls their virtual destructors (important!)
     * but their no-op delete operator leaves them to be purged in one go with the data pool.
     */
    inline Pool* undo() {
        for (auto i = m_undoActions.rbegin();
             i != m_undoActions.rend(); ++i) {
            (*i)->undo();
            delete *i;
        }
        Pool * result = m_dataPool;
        delete this;
        // return the pool for recycling.
        return result;
    }

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
    inline Pool* release() {
        for (auto i = m_undoActions.begin();
             i != m_undoActions.end(); ++i) {
            (*i)->release();
            delete *i;
        }
        for(auto cur = m_interests.begin(); cur != m_interests.end(); ++cur) {
           (*cur)->notifyQuantumRelease();
        }
        Pool* result = m_dataPool;
        delete this;
        // return the pool for recycling.
        return result;
    }

public:
    inline int64_t getUndoToken() const {
        return m_undoToken;
    }

    virtual bool isDummy() {return false;}

    inline int64_t getAllocatedMemory() const
    {
        return m_dataPool->getAllocatedMemory();
    }

    template <typename T> T allocatePooledCopy(T original, std::size_t sz)
    {
        return reinterpret_cast<T>(::memcpy(m_dataPool->allocate(sz), original, sz));
    }

    void* allocateAction(size_t sz) { return m_dataPool->allocate(sz); }

private:
    const int64_t m_undoToken;
    std::deque<UndoReleaseAction*, voltdb::allocator<UndoReleaseAction*>> m_undoActions;
    uint32_t m_numInterests;
    uint32_t m_interestsCapacity;
    std::list<UndoQuantumReleaseInterest*, voltdb::allocator<UndoQuantumReleaseInterest*>> m_interests;
    const bool m_forLowestSite;
protected:
    Pool *m_dataPool;
};


inline void* UndoReleaseAction::operator new(size_t sz, UndoQuantum& uq) { return uq.allocateAction(sz); }

}

#endif /* UNDOQUANTUM_H_ */
