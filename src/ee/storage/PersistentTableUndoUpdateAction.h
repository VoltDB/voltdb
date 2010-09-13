/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef PERSISTENTTABLEUNDOUPDATEACTION_H_
#define PERSISTENTTABLEUNDOUPDATEACTION_H_

#include "common/UndoAction.h"
#include "common/TupleSchema.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"
#include "storage/persistenttable.h"


namespace voltdb {

class PersistentTableUndoUpdateAction: public voltdb::UndoAction {
public:

    inline PersistentTableUndoUpdateAction(
            voltdb::TableTuple &oldTuple,
            voltdb::PersistentTable *table,
            voltdb::Pool *pool)
        : m_oldTuple(oldTuple), m_table(table), m_revertIndexes(false), m_wrapperOffset(0)
    {
        /*
         * Copy the old tuple and the new tuple. The new tuple will be
         * necessary for undo since we need to look it up in the table
         * to update it.
         */
        void *tupleData = pool->allocate(m_oldTuple.tupleLength());
        m_oldTuple.move(tupleData);
        ::memcpy(tupleData, oldTuple.address(), m_oldTuple.tupleLength());
    }

    inline TableTuple& getOldTuple() {
        return m_oldTuple;
    }

    inline void setELMark(size_t mark) {
        m_wrapperOffset = mark;
    }

    inline void setNewTuple(TableTuple &newTuple, voltdb::Pool *pool) {
        m_newTuple = newTuple;
        void *tupleData = pool->allocate(m_newTuple.tupleLength());
        m_newTuple.move(tupleData);
        ::memcpy(tupleData, newTuple.address(), m_newTuple.tupleLength());

        const voltdb::TupleSchema *schema = m_oldTuple.getSchema();
        const uint16_t uninlineableObjectColumnCount = schema->getUninlinedObjectColumnCount();

        /*
         * Record which unlineableObjectColumns were updated so the
         * strings can be freed when this UndoAction is released or
         * undone.
         */
        if (uninlineableObjectColumnCount > 0) {
            for (uint16_t ii = 0; ii < uninlineableObjectColumnCount; ii++) {
                const uint16_t uninlineableObjectColumn = schema->getUninlinedObjectColumnInfoIndex(ii);
                const char *mPtr = *reinterpret_cast<char* const*>
                  (m_oldTuple.getDataPtr(uninlineableObjectColumn));
                const char *oPtr = *reinterpret_cast<char* const*>
                  (m_newTuple.getDataPtr(uninlineableObjectColumn));
                /*
                 * Only need to record the ones that are different and
                 * thus separate allocations.
                 */
                if (mPtr != oPtr) {
                    oldUninlineableColumns.push_back(mPtr);
                    newUninlineableColumns.push_back(oPtr);
                }
            }
        }
    }

    /*
     * Undo whatever this undo action was created to undo. In this
     * case the string allocations of the new tuple must be freed and
     * the tuple must be overwritten with the old one.
     */
    void undo();

    /*
     * Release any resources held by the undo action. It will not need
     * to be undone in the future. In this case the string allocations
     * of the old tuple must be released.
     */
    void release();

    /**
     * After it has been decided to update the indexes the undo
     * quantum needs to be notified
     */
    inline void needToRevertIndexes() {
        m_revertIndexes = true;
    }

    virtual ~PersistentTableUndoUpdateAction();

private:
    voltdb::TableTuple m_oldTuple;
    voltdb::TableTuple m_newTuple;
    voltdb::PersistentTable *m_table;
    std::vector<const char*> oldUninlineableColumns;
    std::vector<const char*> newUninlineableColumns;
    bool m_revertIndexes;
    size_t m_wrapperOffset;
};

}

#endif /* PERSISTENTTABLEUNDOUPDATEACTION_H_ */
