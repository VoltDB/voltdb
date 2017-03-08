/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
#ifndef PERSISTENTTABLEUNDOSWAPTABLEACTION_H_
#define PERSISTENTTABLEUNDOSWAPTABLEACTION_H_

#include "common/UndoAction.h"
#include "storage/persistenttable.h"

namespace voltdb {

class PersistentTableUndoSwapTableAction: public UndoAction {
public:
    PersistentTableUndoSwapTableAction(
            PersistentTable *theTable,
            PersistentTable *otherTable,
            std::vector<std::string> const& theIndexNames,
            std::vector<std::string> const& otherIndexNames,
            DRTupleStreamUndoAction *drUndoAction)
        : m_theTable(theTable)
        , m_otherTable(otherTable)
        , m_theIndexNames(theIndexNames)
        , m_otherIndexNames(otherIndexNames)
        , m_drUndoAction(drUndoAction)
    { }

private:
    virtual ~PersistentTableUndoSwapTableAction() {}

    /*
     * Undo whatever this undo action was created to undo.
     * In this case, swap the tables back to their original state.
     */
    virtual void undo() {
        m_otherTable->swapTable
               (m_theTable,
                m_theIndexNames, m_otherIndexNames,
                false,
                true);
        if (m_drUndoAction) {
            m_drUndoAction->undo();
        }
    }

    /*
     * Release any resources held by the undo action. It will not need to be undone.
     */
    virtual void release() {
        if (m_drUndoAction) {
            m_drUndoAction->release();
        }
    }

private:
    PersistentTable * const m_theTable;
    PersistentTable * const m_otherTable;
    std::vector<std::string> const m_theIndexNames;
    std::vector<std::string> const m_otherIndexNames;
    DRTupleStreamUndoAction * const m_drUndoAction;
};

}// namespace voltdb

#endif /* PERSISTENTTABLEUNDOSWAPTABLEACTION_H_ */
