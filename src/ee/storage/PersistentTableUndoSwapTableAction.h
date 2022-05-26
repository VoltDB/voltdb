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
#ifndef PERSISTENTTABLEUNDOSWAPTABLEACTION_H_
#define PERSISTENTTABLEUNDOSWAPTABLEACTION_H_

#include "common/UndoReleaseAction.h"
#include "storage/persistenttable.h"

namespace voltdb {

class PersistentTableUndoSwapTableAction: public UndoReleaseAction {
public:
    PersistentTableUndoSwapTableAction(
            PersistentTable* theTable,
            PersistentTable* otherTable,
            std::vector<std::string> const& theIndexNames,
            std::vector<std::string> const& otherIndexNames)
        : m_theTable(theTable)
        , m_otherTable(otherTable)
        , m_theIndexNames(theIndexNames)
        , m_otherIndexNames(otherIndexNames)
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
    }

    /*
     * Release any resources held by the undo action. It will not need to be undone.
     */
    virtual void release() {
        ExecutorContext* executorContext = ExecutorContext::getExecutorContext();
        int64_t uniqueId = executorContext->currentUniqueId();
        AbstractDRTupleStream* drStream = executorContext->drStream();
        if (drStream->drStreamStarted()) {
            drStream->endTransaction(uniqueId);
            drStream->extendBufferChain(0);
        }
        AbstractDRTupleStream* drReplicatedStream = executorContext->drReplicatedStream();
        if (drReplicatedStream && drReplicatedStream->drStreamStarted()) {
            drReplicatedStream->endTransaction(uniqueId);
            drReplicatedStream->extendBufferChain(0);
        }
    }

private:
    PersistentTable* const m_theTable;
    PersistentTable* const m_otherTable;
    std::vector<std::string> const m_theIndexNames;
    std::vector<std::string> const m_otherIndexNames;
};

}// namespace voltdb

#endif /* PERSISTENTTABLEUNDOSWAPTABLEACTION_H_ */
