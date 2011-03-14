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

#include <storage/PersistentTableUndoUpdateAction.h>
#include <cassert>

namespace voltdb {

/*
 * Undo whatever this undo action was created to undo. In this case
 * the string allocations of the new tuple must be freed and the tuple
 * must be overwritten with the old one.
 */
void PersistentTableUndoUpdateAction::undo() {
    //Get the address of the tuple in the table and then update it
    //with the old tuple.  If the indexes haven't been updates then it
    //has to be looked up
    TableTuple tupleInTable;
    if (m_revertIndexes) {
        tupleInTable = m_table->lookupTuple(m_newTuple);
    } else {
        //TableScan will find the already updated tuple since the copy
        //is done immediately
        if (m_table->primaryKeyIndex() == NULL) {
            tupleInTable = m_table->lookupTuple(m_newTuple);
        } else {
            //IndexScan will find it under the old tuple entry since the
            //index was never updated
            tupleInTable = m_table->lookupTuple(m_oldTuple);
        }
    }
    m_table->updateTupleForUndo(m_oldTuple, tupleInTable, m_revertIndexes);

    /*
     * Free the strings from the new tuple that updated in the old tuple.
     */
    for (std::vector<const char*>::iterator i = newUninlineableColumns.begin();
         i != newUninlineableColumns.end(); i++)
    {
        NValue::deserializeFromTupleStorage( &(*i), VALUE_TYPE_VARCHAR, false ).free();
    }
}

/*
 * Release any resources held by the undo action. It will not need to
 * be undone in the future. In this case the string allocations of the
 * old tuple must be released.
 */
void PersistentTableUndoUpdateAction::release() {
    /*
     * Free the strings from the old tuple that were updated.
     */
    for (std::vector<const char*>::iterator i = oldUninlineableColumns.begin();
         i != oldUninlineableColumns.end(); i++)
    {
        NValue::deserializeFromTupleStorage( &(*i), VALUE_TYPE_VARCHAR, false ).free();
    }
}

PersistentTableUndoUpdateAction::~PersistentTableUndoUpdateAction() {
}

}
