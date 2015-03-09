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

#ifndef UNDOQUANTUM_H_
#define UNDOQUANTUM_H_

#include <vector>
#include <stdint.h>
#include <cassert>
#include <string.h>

#include "common/Pool.hpp"
#include "common/UndoAction.h"
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
    friend class UndoAction; // For allocateAction.
    friend class ::StreamedTableTest;

protected:
    void* operator new(size_t sz, Pool& pool) { return pool.allocate(sz); }
    void operator delete(void*, Pool&) { /* emergency deallocator does nothing */ }
    void operator delete(void*) { /* every-day deallocator does nothing -- lets the pool cope */ }

    inline UndoQuantum(int64_t undoToken, Pool *dataPool)
        : m_undoToken(undoToken), m_numInterests(0), m_interestsCapacity(0), m_interests(NULL), m_dataPool(dataPool) {}
    inline virtual ~UndoQuantum() {}

public:
    virtual inline void registerUndoAction(UndoAction *undoAction, UndoQuantumReleaseInterest *interest = NULL) {
        assert(undoAction);
        m_undoActions.push_back(undoAction);

        if (interest != NULL) {
            if (m_interests == NULL) {
                m_interests = reinterpret_cast<UndoQuantumReleaseInterest**>(m_dataPool->allocate(sizeof(void*) * 16));
                m_interestsCapacity = 16;
            }
            bool isDup = false;
            for (int ii = 0; ii < m_numInterests; ii++) {
                if (m_interests[ii] == interest) {
                    isDup = true;
                }
            }
            if (!isDup) {
                if (m_numInterests == m_interestsCapacity) {
                    UndoQuantumReleaseInterest **newStorage =
                            reinterpret_cast<UndoQuantumReleaseInterest**>(m_dataPool->allocate(sizeof(void*) * m_interestsCapacity * 2));
                    ::memcpy(newStorage, m_interests, sizeof(void*) * m_interestsCapacity);
                    m_interests = newStorage;
                    m_interestsCapacity *= 2;
                }
                m_interests[m_numInterests++] = interest;
            }
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
        for (std::vector<UndoAction*>::reverse_iterator i = m_undoActions.rbegin();
             i != m_undoActions.rend(); ++i) {
            UndoAction* goner = *i;
            goner->undo();
            delete goner;
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
     */
    inline Pool* release() {
        for (std::vector<UndoAction*>::reverse_iterator i = m_undoActions.rbegin();
             i != m_undoActions.rend(); ++i) {
            UndoAction* goner = *i;
            goner->release();
            delete goner;
        }
        if (m_interests != NULL) {
            for (int ii = 0; ii < m_numInterests; ii++) {
                m_interests[ii]->notifyQuantumRelease();
            }
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
    std::vector<UndoAction*> m_undoActions;
    uint32_t m_numInterests;
    uint32_t m_interestsCapacity;
    UndoQuantumReleaseInterest **m_interests;
protected:
    Pool *m_dataPool;
};


inline void* UndoAction::operator new(size_t sz, UndoQuantum& uq) { return uq.allocateAction(sz); }

}

#endif /* UNDOQUANTUM_H_ */
