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

#ifndef PERSISTENTTABLEUNDOINSERTACTION_H_
#define PERSISTENTTABLEUNDOINSERTACTION_H_

#include "common/UndoAction.h"
#include "storage/persistenttable.h"

namespace voltdb {


class PersistentTableUndoInsertAction: public voltdb::UndoAction {
public:
    inline PersistentTableUndoInsertAction(char* insertedTuple,
                                           voltdb::PersistentTableSurgeon *table,
                                           size_t drMark)
        : m_tuple(insertedTuple), m_table(table), m_drMark(drMark)
    { }

    virtual ~PersistentTableUndoInsertAction() { }

    /*
     * Undo whatever this undo action was created to undo
     */
    virtual void undo() {
        m_table->deleteTupleForUndo(m_tuple);
        m_table->DRRollback(m_drMark);
    }

    /*
     * Release any resources held by the undo action. It will not need
     * to be undone in the future.
     */
    void release() { }
private:
    char* m_tuple;
    PersistentTableSurgeon *m_table;
    size_t m_drMark;
};

}

#endif /* PERSISTENTTABLEUNDOINSERTACTION_H_ */
