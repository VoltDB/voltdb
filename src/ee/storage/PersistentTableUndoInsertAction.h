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

#ifndef PERSISTENTTABLEUNDOINSERTACTION_H_
#define PERSISTENTTABLEUNDOINSERTACTION_H_

#include "common/UndoAction.h"
#include "common/TupleSchema.h"
#include "common/Pool.hpp"
#include "common/tabletuple.h"
#include "storage/persistenttable.h"

namespace voltdb {


class PersistentTableUndoInsertAction: public voltdb::UndoAction {
public:
    inline PersistentTableUndoInsertAction(voltdb::TableTuple insertedTuple,
                                           voltdb::PersistentTable *table,
                                           voltdb::Pool *pool,
                                           size_t wrapperOffset)
        : m_tuple(insertedTuple), m_table(table), m_wrapperOffset(wrapperOffset)
    {
        void *tupleData = pool->allocate(m_tuple.tupleLength());
        m_tuple.move(tupleData);
        ::memcpy(tupleData, insertedTuple.address(), m_tuple.tupleLength());
    }

    virtual ~PersistentTableUndoInsertAction();

    /*
     * Undo whatever this undo action was created to undo
     */
    void undo();

    /*
     * Release any resources held by the undo action. It will not need
     * to be undone in the future.
     */
    void release();
private:
    voltdb::TableTuple m_tuple;
    PersistentTable *m_table;
    size_t m_wrapperOffset;
};

}

#endif /* PERSISTENTTABLEUNDOINSERTACTION_H_ */
