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

#pragma once

#include "common/UndoReleaseAction.h"
#include "common/types.h"
#include "storage/persistenttable.h"

namespace voltdb {


class PersistentTableUndoDeleteAction: public UndoReleaseAction {
    char *m_tuple;
    PersistentTableSurgeon *m_table;

    virtual ~PersistentTableUndoDeleteAction() { }

    /*
     * Undo whatever this undo action was created to undo. In this case reinsert the tuple into the table.
     */
    virtual void undo() {
        m_table->insertTupleForUndo(m_tuple);
    }

    /*
     * Release any resources held by the undo action. It will not need to be undone in the future.
     * In this case free the strings associated with the tuple.
     */
    virtual void release() { m_table->deleteTupleRelease(m_tuple); }
public:
    inline PersistentTableUndoDeleteAction(char *deletedTuple, PersistentTableSurgeon *table)
        : m_tuple(deletedTuple), m_table(table) {}
};

}

