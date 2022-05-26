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

#ifndef DRTUPLESTREAMUNDOACTION_H
#define DRTUPLESTREAMUNDOACTION_H

#include "common/UndoReleaseAction.h"

namespace voltdb {

class DRTupleStreamUndoAction : public UndoOnlyAction {
public:
DRTupleStreamUndoAction(AbstractDRTupleStream *stream, size_t mark, size_t cost)
    : m_stream(stream), m_mark(mark), m_cost(cost)
    {
        vassert(stream);
    }

    void undo() {
        m_stream->rollbackDrTo(m_mark, m_cost);
    }

private:
    AbstractDRTupleStream *m_stream;
    size_t m_mark;
    size_t m_cost;
};

}

#endif
