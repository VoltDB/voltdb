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

#ifndef UNDOQUANTUM_H_
#define UNDOQUANTUM_H_

#include <stdint.h>
#include <common/debuglog.h>
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
    friend class ::StreamedTableTest;

protected:
    void* operator new(size_t sz, Pool& pool) { return pool.allocate(sz); }
    void operator delete(void*, Pool&) { /* emergency deallocator does nothing */ }
    void operator delete(void*) { /* every-day deallocator does nothing -- lets the pool cope */ }


public:
    inline UndoQuantum(int64_t undoToken, Pool *dataPool)
        : m_undoToken(undoToken), m_dataPool(dataPool) {}
    inline virtual ~UndoQuantum() {}

    /**
     * Add a new UndoReleaseAction to the list of undo actions. interest is an optional UndoQuantumReleaseInterest which
     * will be added to the list of interested parties and invoked upon release of the undo quantum after all
     * undoActions have been performed.
     */
    inline void registerUndoAction(UndoReleaseAction *undoAction, UndoQuantumReleaseInterest *interest = NULL) {
        vassert(undoAction);
        m_undoActions.push_back(undoAction);

        if (interest != NULL && interest->isNewReleaseInterest(m_undoToken)) {
           m_interests.push_back(interest);
        }
    }

    inline void registerSynchronizedUndoAction(UndoReleaseAction *undoAction, UndoQuantumReleaseInterest *interest = NULL) {
        vassert(undoAction);
        m_undoActions.push_back(undoAction);

        if (interest != NULL) {
           m_interests.push_back(interest);
        }
    }

    /**
     * removeInterest is an UndoQuantumReleaseInterest which will be removed
     * from the list of interested parties if it had been previously added.
     */
    inline void unregisterReleaseInterest(UndoQuantumReleaseInterest *removeInterest) {
        m_interests.remove(removeInterest);
    }

    /*
     * Invoke all the undo actions for this UndoQuantum. UndoActions
     * must have released all memory after undo() is called.
     * "delete" here only really calls their virtual destructors (important!)
     * but their no-op delete operator leaves them to be purged in one go with the data pool.
     */
    static Pool* undo(UndoQuantum&& quantum) {
        for (auto i = quantum.m_undoActions.rbegin(); i != quantum.m_undoActions.rend(); ++i) {
            (*i)->undo();
            delete *i;
        }
        Pool * result = quantum.m_dataPool;
        quantum.~UndoQuantum();
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
    static Pool* release(UndoQuantum&& quantum) {
        for (auto i = quantum.m_undoActions.begin();
             i != quantum.m_undoActions.end(); ++i) {
            (*i)->release();
            delete *i;
        }
        for(auto cur = quantum.m_interests.begin(); cur != quantum.m_interests.end(); ++cur) {
           (*cur)->notifyQuantumRelease();
        }
        Pool* result = quantum.m_dataPool;
        quantum.~UndoQuantum();
        // return the pool for recycling.
        return result;
    }

    inline int64_t getUndoToken() const {
        return m_undoToken;
    }

    inline int64_t getAllocatedMemory() const
    {
        return m_dataPool->getAllocatedMemory();
    }
    Pool* getPool() const throw() {
       return m_dataPool;
    }

    inline const UndoReleaseAction* getLastUndoActionForTest() { return m_undoActions.back(); }

    void* allocateAction(size_t sz) { return m_dataPool->allocate(sz); }
private:
    const int64_t m_undoToken;
    std::deque<UndoReleaseAction*> m_undoActions;
    std::list<UndoQuantumReleaseInterest*> m_interests;
protected:
    Pool *m_dataPool;
};


inline void* UndoReleaseAction::operator new(size_t sz, UndoQuantum& uq) { return uq.allocateAction(sz); }

}

#endif /* UNDOQUANTUM_H_ */
