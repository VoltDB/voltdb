/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

namespace voltdb {


UndoReleaseAction* UndoReleaseAction::getSynchronizeUndoAction(UndoQuantum* currUQ) {
    return (new (*currUQ) SynchronizedUndoReleaseAction(this));
}

UndoReleaseAction* UndoReleaseAction::getDummySynchronizeUndoAction(UndoQuantum* currUQ) {
    return (new (*currUQ) SynchronizedDummyUndoReleaseAction());
}

UndoReleaseAction* UndoOnlyAction::getSynchronizeUndoAction(UndoQuantum* currUQ) {
    return (new (*currUQ) SynchronizedUndoOnlyAction(this));
}

UndoReleaseAction* UndoOnlyAction::getDummySynchronizeUndoAction(UndoQuantum* currUQ) {
    return (new (*currUQ) SynchronizedDummyUndoOnlyAction());
}

UndoReleaseAction* ReleaseOnlyAction::getSynchronizeUndoAction(UndoQuantum* currUQ) {
    return (new (*currUQ) SynchronizedReleaseOnlyAction(this));
}

UndoReleaseAction* ReleaseOnlyAction::getDummySynchronizeUndoAction(UndoQuantum* currUQ) {
    return (new (*currUQ) SynchronizedDummyReleaseOnlyAction());
}

}
