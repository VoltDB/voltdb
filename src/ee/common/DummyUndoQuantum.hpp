/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/UndoAction.h"
#include "common/Pool.hpp"
#include <stdint.h>

#ifndef DUMMYUNDOQUANTUM_HPP_
#define DUMMYUNDOQUANTUM_HPP_

/*
 * Implementation of an UndoQuantum that immediately releases and
 * destructs the UndoActions as they are registered. Useful as the
 * default UndoQuantum when the UndoLog is not being used/manage.
 */
namespace voltdb {
class DummyUndoQuantum : public UndoQuantum {

public:
    DummyUndoQuantum() : UndoQuantum( INT64_MIN + 1, new Pool()) {}
    ~DummyUndoQuantum() {
        delete m_dataPool;
    }
    void registerUndoAction(UndoAction *undoAction) {
        undoAction->release();
        undoAction->~UndoAction();
        m_dataPool->purge();
    }
    inline bool isDummy() {return true;}
};
}

#endif /* DUMMYUNDOQUANTUM_HPP_ */
