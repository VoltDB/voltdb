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

#include "UndoReleaseAction.h"
#include "UndoQuantum.h"
#include "SynchronizedThreadLock.h"
#include "ExecuteWithMpMemory.h"

namespace voltdb {

void SynchronizedUndoReleaseAction::undo() {
    vassert(!SynchronizedThreadLock::isInSingleThreadMode());
    SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
    {
        ExecuteWithMpMemory usingMpMemory;
        m_realAction->undo();
    }
    SynchronizedThreadLock::signalLowestSiteFinished();
}

void SynchronizedUndoReleaseAction::release() {
    vassert(!SynchronizedThreadLock::isInSingleThreadMode());
    SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
    {
        ExecuteWithMpMemory usingMpMemory;
        m_realAction->release();
    }
    SynchronizedThreadLock::signalLowestSiteFinished();
}

void SynchronizedUndoOnlyAction::undo() {
    vassert(!SynchronizedThreadLock::isInSingleThreadMode());
    SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
    {
        ExecuteWithMpMemory usingMpMemory;
        m_realAction->undo();
    }
    SynchronizedThreadLock::signalLowestSiteFinished();

}

void SynchronizedReleaseOnlyAction::release() {
    vassert(!SynchronizedThreadLock::isInSingleThreadMode());
    SynchronizedThreadLock::countDownGlobalTxnStartCount(true);
    {
        ExecuteWithMpMemory usingMpMemory;
        m_realAction->release();
    }
    SynchronizedThreadLock::signalLowestSiteFinished();
}

void SynchronizedDummyUndoReleaseAction::undo() {
    vassert(!SynchronizedThreadLock::isInSingleThreadMode());
    SynchronizedThreadLock::countDownGlobalTxnStartCount(false);

}

void SynchronizedDummyUndoReleaseAction::release() {
    vassert(!SynchronizedThreadLock::isInSingleThreadMode());
    SynchronizedThreadLock::countDownGlobalTxnStartCount(false);
}

void SynchronizedDummyUndoOnlyAction::undo() {
    vassert(!SynchronizedThreadLock::isInSingleThreadMode());
    SynchronizedThreadLock::countDownGlobalTxnStartCount(false);
}

void SynchronizedDummyReleaseOnlyAction::release() {
    vassert(!SynchronizedThreadLock::isInSingleThreadMode());
    SynchronizedThreadLock::countDownGlobalTxnStartCount(false);
}

UndoReleaseAction* UndoReleaseAction::getSynchronizedUndoAction(UndoQuantum* currUQ) {
    return (new (*currUQ) SynchronizedUndoReleaseAction(this));
}

UndoReleaseAction* UndoReleaseAction::getDummySynchronizedUndoAction(UndoQuantum* currUQ) {
    return (new (*currUQ) SynchronizedDummyUndoReleaseAction());
}

UndoReleaseAction* UndoOnlyAction::getSynchronizedUndoAction(UndoQuantum* currUQ) {
    return (new (*currUQ) SynchronizedUndoOnlyAction(this));
}

UndoReleaseAction* UndoOnlyAction::getDummySynchronizedUndoAction(UndoQuantum* currUQ) {
    return (new (*currUQ) SynchronizedDummyUndoOnlyAction());
}

UndoReleaseAction* ReleaseOnlyAction::getSynchronizedUndoAction(UndoQuantum* currUQ) {
    return (new (*currUQ) SynchronizedReleaseOnlyAction(this));
}

UndoReleaseAction* ReleaseOnlyAction::getDummySynchronizedUndoAction(UndoQuantum* currUQ) {
    return (new (*currUQ) SynchronizedDummyReleaseOnlyAction());
}

}
