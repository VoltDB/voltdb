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


#ifndef UNDOACTION_H_
#define UNDOACTION_H_

#include <cstdlib>

namespace voltdb {
class UndoQuantum;

/*
 * Abstract base class for all classes generated to undo changes to the system.
 * Always memory-managed by and registered with an undo quantum.
 */
class UndoAction {
public:
    void* operator new(std::size_t sz, UndoQuantum& uq); // defined inline in UndoQuantum.h
    void operator delete(void*, UndoQuantum&) { /* emergency deallocator does nothing */ }
    void operator delete(void*) { /* every-day deallocator does nothing -- lets the pool cope */ }

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
