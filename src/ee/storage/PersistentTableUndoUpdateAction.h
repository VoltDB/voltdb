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

#ifndef PERSISTENTTABLEUNDOUPDATEACTION_H_
#define PERSISTENTTABLEUNDOUPDATEACTION_H_

#include "common/UndoAction.h"
#include "common/NValue.hpp"
#include "storage/persistenttable.h"


namespace voltdb {

class PersistentTableUndoUpdateAction: public UndoAction {
public:

    inline PersistentTableUndoUpdateAction(char* oldTuple, char* newTuple,
                                           std::vector<char*> const & oldObjects, std::vector<char*> const & newObjects,
                                           PersistentTableSurgeon *table, bool revertIndexes, size_t drMark)
      : m_oldTuple(oldTuple), m_newTuple(newTuple),
        m_table(table), m_revertIndexes(revertIndexes),
        m_oldUninlineableColumns(oldObjects), m_newUninlineableColumns(newObjects), m_drMark(drMark)
    { }

    /*
     * Undo whatever this undo action was created to undo. In this
     * case the string allocations of the new tuple must be freed and
     * the tuple must be overwritten with the old one.
     */
    virtual void undo()
    {
        m_table->updateTupleForUndo(m_newTuple, m_oldTuple, m_revertIndexes);
        NValue::freeObjectsFromTupleStorage(m_newUninlineableColumns);
        m_table->DRRollback(m_drMark);
    }

    /*
     * Release any resources held by the undo action. It will not need
     * to be undone in the future. In this case the string allocations
     * of the old tuple must be released.
     */
    virtual void release() { NValue::freeObjectsFromTupleStorage(m_oldUninlineableColumns); }

    virtual ~PersistentTableUndoUpdateAction() { }

private:
    char* const m_oldTuple;
    char* const m_newTuple;
    PersistentTableSurgeon * const m_table;
    bool const m_revertIndexes;
    std::vector<char*> const m_oldUninlineableColumns;
    std::vector<char*> const m_newUninlineableColumns;
    size_t const m_drMark;
};

}

#endif /* PERSISTENTTABLEUNDOUPDATEACTION_H_ */
