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
#ifndef PERSISTENTTABLEUNDOTRUNCATETABLEACTION_H_
#define PERSISTENTTABLEUNDOTRUNCATETABLEACTION_H_

#include "common/UndoReleaseAction.h"
#include "storage/persistenttable.h"

namespace voltdb {

class PersistentTableUndoTruncateTableAction: public UndoReleaseAction {
public:
    PersistentTableUndoTruncateTableAction(TableCatalogDelegate * tcd,
            PersistentTable *originalTable,
            PersistentTable *emptyTable)
        : m_tcd(tcd)
        , m_originalTable(originalTable)
        , m_emptyTable(emptyTable)
    {}

private:
    virtual ~PersistentTableUndoTruncateTableAction() {}

    /*
     * Undo the original action.
     * In this case, delete the newly constructed empty table,
     * and re-associate the table delegate with the original table.
     *
     */
    virtual void undo() {
        m_emptyTable->truncateTableUndo(m_tcd, m_originalTable);
    }

    /*
     * Release any resources held by the undo action,
     * because the action will not need to be undone.
     * In this case, delete all tuples from indexes and views
     * and free the strings associated with each
     * tuple in the original table.
     */
    virtual void release() {
        //It's very important not to add anything else to this release method
        //Put all the implementation in truncateTableRelease
        //The reason is that truncateTableRelease is called directly when a binary log
        //truncate record is being applied and it must do all the work and not leave
        //something undone because it didn't go through this undo action
        m_emptyTable->truncateTableRelease(m_originalTable);
    }

private:
    TableCatalogDelegate* m_tcd;
    PersistentTable* m_originalTable;
    PersistentTable* m_emptyTable;
};

}// namespace voltdb

#endif /* PERSISTENTTABLEUNDOTRUNCATETABLEACTION_H_ */
