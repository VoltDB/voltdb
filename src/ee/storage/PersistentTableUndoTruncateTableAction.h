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
#ifndef PERSISTENTTABLEUNDOTRUNCATETABLEACTION_H_
#define PERSISTENTTABLEUNDOTRUNCATETABLEACTION_H_

#include "common/UndoAction.h"
#include "storage/persistenttable.h"

namespace voltdb {

class PersistentTableUndoTruncateTableAction: public UndoAction {
public:
    inline PersistentTableUndoTruncateTableAction(VoltDBEngine * engine, TableCatalogDelegate * tcd,
            PersistentTable *originalTable, PersistentTable *emptyTable)
    :  m_engine(engine), m_tcd(tcd), m_originalTable(originalTable), m_emptyTable(emptyTable)
    {}

private:
    virtual ~PersistentTableUndoTruncateTableAction() {}

    /*
     * Undo whatever this undo action was created to undo. In this case delete the newly constructed table,
     * and assign the table delegate with the original table.
     *
     */
    virtual void undo() {
        m_emptyTable->truncateTableForUndo(m_engine, m_tcd, m_originalTable);
    }

    /*
     * Release any resources held by the undo action. It will not need to be undone in the future.
     * In this case delete all tuples from indexes, views and free the strings associated with each
     * tuple in the original table.
     */
    virtual void release() {
        m_emptyTable->truncateTableRelease(m_originalTable);
    }

private:
    VoltDBEngine * m_engine;
    TableCatalogDelegate * m_tcd;
    PersistentTable *m_originalTable;
    PersistentTable *m_emptyTable;
};

}

#endif /* PERSISTENTTABLEUNDOTRUNCATETABLEACTION_H_ */
