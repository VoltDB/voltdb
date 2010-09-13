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

#ifndef PERSISTENTTABLEUNDODELETEACTION_H_
#define PERSISTENTTABLEUNDODELETEACTION_H_

#include "common/UndoAction.h"
#include "common/TupleSchema.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"
#include "persistenttable.h"


namespace voltdb {

class PersistentTableUndoDeleteAction: public voltdb::UndoAction {
public:
    inline PersistentTableUndoDeleteAction(voltdb::TableTuple deletedTuple,
                                           voltdb::PersistentTable *table, voltdb::Pool *pool)
        : m_tuple(deletedTuple), m_table(table), m_wrapperOffset(0)
    {
        void *tupleData = pool->allocate(m_tuple.tupleLength());
        m_tuple.move(tupleData);
        ::memcpy(tupleData, deletedTuple.address(), m_tuple.tupleLength());
    }

    virtual ~PersistentTableUndoDeleteAction();

    void setELMark(size_t mark) {
        m_wrapperOffset = mark;
    }

    /*
     * Undo whatever this undo action was created to undo. In this case reinsert the tuple into the table.
     */
    void undo();

    /*
     * Release any resources held by the undo action. It will not need to be undone in the future.
     * In this case free the strings associated with the tuple.
     */
    void release();
private:
    voltdb::TableTuple m_tuple;
    PersistentTable *m_table;
    size_t m_wrapperOffset;
};

}

#endif /* PERSISTENTTABLEUNDODELETEACTION_H_ */
