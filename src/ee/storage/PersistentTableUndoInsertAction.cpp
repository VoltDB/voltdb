/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

#include "storage/PersistentTableUndoInsertAction.h"

namespace voltdb {

/*
 * Undo whatever this undo action was created to undo
 */
void PersistentTableUndoInsertAction::undo() {
    m_table->deleteTupleForUndo(m_tuple);
}

/*
 * Release any resources held by the undo action. It will not need to be undone in the future.
 */
void PersistentTableUndoInsertAction::release() {
    /*
     * Do nothing. Tuple stays inserted so no memory needs to be released.
     */
}

PersistentTableUndoInsertAction::~PersistentTableUndoInsertAction() {
    // TODO Auto-generated destructor stub
}

}
