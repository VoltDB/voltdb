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

#ifndef STREAMEDTABLEUNDOACTION_HPP
#define STREAMEDTABLEUNDOACTION_HPP

#include "common/UndoReleaseAction.h"

namespace voltdb {

class StreamedTableUndoAction : public UndoOnlyAction {

  public:

    StreamedTableUndoAction(StreamedTable *table, size_t mark, int64_t seqNo)
        : m_table(table), m_mark(mark), m_seqNo(seqNo)
    {
    }

    void undo() {
        m_table->undo(m_mark, m_seqNo);
    }

  private:
    StreamedTable *m_table;
    size_t m_mark;
    int64_t m_seqNo;

};

}

#endif
