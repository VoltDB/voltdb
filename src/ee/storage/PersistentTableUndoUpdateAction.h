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
#include "storage/persistenttable.h"

namespace voltdb {

class PersistentTableUndoUpdateAction: public UndoReleaseAction {
    char* const m_oldTuple;
    char* const m_newTuple;
    PersistentTableSurgeon * const m_tableSurgeon;
    bool const m_revertIndexes;
    std::vector<char*> const m_oldUninlineableColumns;
    std::vector<char*> const m_newUninlineableColumns;
    bool const m_updateMigrate;
public:
    PersistentTableUndoUpdateAction(char* oldTuple,
                                    char* newTuple,
                                    std::vector<char*> const & oldObjects,
                                    std::vector<char*> const & newObjects,
                                    PersistentTableSurgeon *tableSurgeon,
                                    bool revertIndexes,
                                    bool updateMigrate)
        : m_oldTuple(oldTuple)
        , m_newTuple(newTuple)
        , m_tableSurgeon(tableSurgeon)
        , m_revertIndexes(revertIndexes)
        , m_oldUninlineableColumns(oldObjects)
        , m_newUninlineableColumns(newObjects)
        , m_updateMigrate(updateMigrate)
    { }

    /*
     * Undo whatever this undo action was created to undo. In this
     * case the string allocations of the new tuple must be freed and
     * the tuple must be overwritten with the old one.
     */
    virtual void undo() {
        m_tableSurgeon->updateTupleForUndo(m_newTuple, m_oldTuple, m_revertIndexes, m_updateMigrate);
        NValue::freeObjectsFromTupleStorage(m_newUninlineableColumns);
    }

    /*
     * Release any resources held by the undo action. It will not need
     * to be undone in the future. In this case the string allocations
     * of the old tuple must be released.
     */
    virtual void release() {
        NValue::freeObjectsFromTupleStorage(m_oldUninlineableColumns);
    }
    virtual ~PersistentTableUndoUpdateAction() { }
};

}

