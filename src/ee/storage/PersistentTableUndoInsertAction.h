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

class PersistentTableUndoInsertAction: public UndoOnlyAction {
    char* m_tuple;
    PersistentTableSurgeon *m_tableSurgeon;
public:
    PersistentTableUndoInsertAction(
          char* insertedTuple, voltdb::PersistentTableSurgeon *tableSurgeon) :
       m_tuple(insertedTuple), m_tableSurgeon(tableSurgeon) { }

    virtual ~PersistentTableUndoInsertAction() { }

    /*
     * Undo whatever this undo action was created to undo
     */
    void undo() override {
       m_tableSurgeon->deleteTupleForUndo(m_tuple);
    }
    char const* getTupleForTest() const {
       return m_tuple;
    }
};

}

