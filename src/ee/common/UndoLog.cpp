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

#include <common/UndoLog.h>
#include <stdint.h>
#include <iostream>

namespace voltdb {

UndoLog::UndoLog()
  : m_lastUndoToken(INT64_MIN), m_lastReleaseToken(INT64_MIN)
{
}

void UndoLog::clear()
{
    if (m_undoQuantums.size() > 0) {
        release(m_lastUndoToken);
    }
    for (std::vector<Pool*>::iterator i = m_undoDataPools.begin();
         i != m_undoDataPools.end();
         i++) {
        delete *i;
    }
    m_undoDataPools.clear();
    m_undoQuantums.clear();
}

UndoLog::~UndoLog()
{
    clear();
}

}
