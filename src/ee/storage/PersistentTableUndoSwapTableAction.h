/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
    inline PersistentTableUndoSwapTableAction(
            PersistentTable *originalTable,
            PersistentTable *otherTable)
    : m_originalTable(originalTable)
    , m_otherTable(otherTable)
    {}

private:
    virtual ~PersistentTableUndoSwapTableAction() {}

    /*
     * Undo whatever this undo action was created to undo.
     * In this case, swap the tables back to their original state.
     */
    virtual void undo() {
        VoltDBEngine* engine = ExecutorContext::getEngine();
        m_originalTable->swapTable(m_otherTable, engine, false);
    }

    /*
     * Release any resources held by the undo action. It will not need to be undone.
     */
    virtual void release() { }

private:
    PersistentTable *m_originalTable;
    PersistentTable *m_otherTable;
};

}// namespace voltdb

#endif /* PERSISTENTTABLEUNDOSWAPTABLEACTION_H_ */
