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

#include "storage/PersistentTableUndoDeleteAction.h"

namespace voltdb {

/*
 * Undo whatever this undo action was created to undo. In this case
 * reinsert the tuple into the table.
 */
void PersistentTableUndoDeleteAction::undo() {
    m_table->insertTupleForUndo(m_tuple, m_wrapperOffset);
}

/*
 * Release any resources held by the undo action. It will not need to
 * be undone in the future.  In this case free the strings associated
 * with the tuple.
 */
void PersistentTableUndoDeleteAction::release() {
    /*
     * Before deleting the tuple free any allocated strings.
     * Persistent tables are responsible for managing the life of
     * strings stored in the table.
     */
    m_tuple.freeObjectColumns();
}

PersistentTableUndoDeleteAction::~PersistentTableUndoDeleteAction() {
    // TODO Auto-generated destructor stub
}

}
