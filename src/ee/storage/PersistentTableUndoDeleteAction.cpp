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

#include "storage/PersistentTableUndoDeleteAction.h"
#include "storage/persistenttable.h"

namespace voltdb {

/*
 * Undo whatever this undo action was created to undo. In this case
 * reinsert the tuple into the table.
 */
void PersistentTableUndoDeleteAction::undo() {
    TableTuple tuple( m_tuple, m_table->schema());
    m_table->insertTupleForUndo(m_tuple);
}

/*
 * Release any resources held by the undo action. It will not need to
 * be undone in the future.  In this case free the strings associated
 * with the tuple.
 */
void PersistentTableUndoDeleteAction::release() {
    TableTuple tuple( m_tuple, m_table->schema());
    tuple.setPendingDeleteOnUndoReleaseFalse();
    m_table->m_tuplesPinnedByUndo--;

    /*
     * Before deleting the tuple free any allocated strings.
     * Persistent tables are responsible for managing the life of
     * strings stored in the table.
     */
    if (m_table->m_COWContext == NULL) {
        //No snapshot in progress, just whack it
        if (m_table->m_schema->getUninlinedObjectColumnCount() != 0)
        {
            m_table->decreaseStringMemCount(tuple.getNonInlinedMemorySize());
        }
        tuple.freeObjectColumns();
        m_table->deleteTupleStorage(tuple);
    } else {
        if (m_table->m_COWContext->canSafelyFreeTuple(tuple)) {
            //Safe to free the tuple and do memory accounting
            if (m_table->m_schema->getUninlinedObjectColumnCount() != 0)
            {
                m_table->decreaseStringMemCount(tuple.getNonInlinedMemorySize());
            }
            tuple.freeObjectColumns();
            m_table->deleteTupleStorage(tuple);
        } else {
            //Mark it pending delete and let the snapshot land the finishing blow
            tuple.setPendingDeleteTrue();
        }
    }
}

PersistentTableUndoDeleteAction::~PersistentTableUndoDeleteAction() {
    // TODO Auto-generated destructor stub
}

}
