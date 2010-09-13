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


#ifndef UNDOACTION_H_
#define UNDOACTION_H_

namespace voltdb {

/*
 * Abstract base class for all classes generated to undo changes to the system. Can be registered with an
 * undo quantum
 */
class UndoAction {
public:
    inline UndoAction() {}
    virtual ~UndoAction() {}

    /*
     * Undo whatever this undo action was created to undo
     */
    virtual void undo() = 0;

    /*
     * Release any resources held by the undo action. It will not need to be undone in the future.
     */
    virtual void release() = 0;
};
}
#endif /* UNDOACTION_H_ */
