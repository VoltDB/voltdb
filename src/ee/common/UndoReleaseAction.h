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


#ifndef UNDORELEASEACTION_H_
#define UNDORELEASEACTION_H_

#include <cstdlib>

namespace voltdb {
class UndoQuantum;

/*
 * Abstract base class for all classes generated to undo changes to the system.
 * Always memory-managed by and registered with an undo quantum.
 */
class UndoReleaseAction {
public:
    void* operator new(std::size_t sz, UndoQuantum& uq); // defined inline in UndoQuantum.h
    void operator delete(void*, UndoQuantum&) { /* emergency deallocator does nothing */ }
    void operator delete(void*) { /* every-day deallocator does nothing -- lets the pool cope */ }

    inline UndoReleaseAction() {}
    virtual ~UndoReleaseAction() {}

    /*
     * Undo whatever this undo action was created to undo
     */
    virtual void undo() = 0;

    /*
     * Release any resources held by the undo action. It will not need to be undone in the future.
     */
    virtual void release() = 0;

    /*
     * Generate a synchronized Version of UndoAction
     */
    virtual UndoReleaseAction* getSynchronizedUndoAction(UndoQuantum* currUQ);
    virtual UndoReleaseAction* getDummySynchronizedUndoAction(UndoQuantum* currUQ);
};

class UndoOnlyAction : public UndoReleaseAction {
public:
    inline UndoOnlyAction() {}
    virtual ~UndoOnlyAction() {}

    /*
     * Release any resources held by the undo action. It will not need to be undone in the future.
     */
    void release() {}

    virtual UndoReleaseAction* getSynchronizedUndoAction(UndoQuantum* currUQ);
    virtual UndoReleaseAction* getDummySynchronizedUndoAction(UndoQuantum* currUQ);
};

class ReleaseOnlyAction : public UndoReleaseAction {
public:
    inline ReleaseOnlyAction() {}
    virtual ~ReleaseOnlyAction() {}

    /*
     * Undo whatever this undo action was created to undo. It will not need to be released in the future.
     */
    void undo() {}

    virtual UndoReleaseAction* getSynchronizedUndoAction(UndoQuantum* currUQ);
    virtual UndoReleaseAction* getDummySynchronizedUndoAction(UndoQuantum* currUQ);
};

class SynchronizedUndoReleaseAction : public UndoReleaseAction {
public:
    SynchronizedUndoReleaseAction(UndoReleaseAction *realAction) : m_realAction(realAction) {}
    virtual ~SynchronizedUndoReleaseAction() {delete m_realAction;}

    void undo();

    void release();

private:
    UndoReleaseAction *m_realAction;
};

class SynchronizedUndoOnlyAction : public UndoOnlyAction {
public:
    SynchronizedUndoOnlyAction(UndoOnlyAction *realAction) : m_realAction(realAction) {}
    virtual ~SynchronizedUndoOnlyAction() {delete m_realAction;}

    void undo();

private:
    UndoOnlyAction *m_realAction;
};

class SynchronizedReleaseOnlyAction : public ReleaseOnlyAction {
public:
    SynchronizedReleaseOnlyAction(ReleaseOnlyAction *realAction) : m_realAction(realAction) {}
    virtual ~SynchronizedReleaseOnlyAction() {delete m_realAction;}

    void release();

private:
    ReleaseOnlyAction *m_realAction;
};

class SynchronizedDummyUndoReleaseAction : public UndoReleaseAction {
public:
    SynchronizedDummyUndoReleaseAction() { }
    virtual ~SynchronizedDummyUndoReleaseAction() { }

    void undo();

    void release();
};

class SynchronizedDummyUndoOnlyAction : public UndoOnlyAction {
public:
    SynchronizedDummyUndoOnlyAction() { }
    virtual ~SynchronizedDummyUndoOnlyAction() { }

    void undo();
};

class SynchronizedDummyReleaseOnlyAction : public ReleaseOnlyAction {
public:
    SynchronizedDummyReleaseOnlyAction() { }
    virtual ~SynchronizedDummyReleaseOnlyAction() { }

    void release();
};

}
#endif /* UNDORELEASEACTION_H_ */
