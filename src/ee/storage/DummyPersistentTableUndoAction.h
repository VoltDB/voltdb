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

#ifndef DUMMYPERSISTENTTABLEUNDOACTION_H_
#define DUMMYPERSISTENTTABLEUNDOACTION_H_

#include "common/UndoAction.h"
#include "common/types.h"
#include "storage/persistenttable.h"

namespace voltdb {


class DummyPersistentTableUndoAction: public voltdb::UndoAction {
public:
    inline DummyPersistentTableUndoAction(voltdb::PersistentTableSurgeon *table)
        : m_table(table)
    { }

    virtual ~DummyPersistentTableUndoAction() { }

    /*
     * Undo whatever this undo action was created to undo
     */
    virtual void undo() {
    }

    /*
     * Release any resources held by the undo action. It will not need
     * to be undone in the future.
     */
    virtual void release() { }

    /*
     * Indicates this undo action needs to be coordinated across sites in the same host
     */
    virtual bool isReplicatedTable() { return m_table->getTable().isReplicatedTable(); }

private:
    PersistentTableSurgeon *m_table;
};

}

#endif
