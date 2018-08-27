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
#include "ExecuteWithMpMemory.h"
#include "SynchronizedThreadLock.h"

using namespace voltdb;

/**
 * These classes are for implementation use only, hence do not
 * put in .h file.
 * TODO: the mixture use of mix-in and inheritance is strange,
 * because the way it achieves polymorphsim is, strange...
 *
 * It does this via getXXXAction() method, that uses placement
 * new to cast to concrete sub-type, and from there achieve
 * polymorphic behaviors of undo/release methods.
 */
class SynchronizedUndoReleaseAction : public UndoReleaseAction {
    UndoReleaseAction* m_realAction;
public:
    SynchronizedUndoReleaseAction(UndoReleaseAction* realAction) : m_realAction(realAction) {}
    virtual ~SynchronizedUndoReleaseAction() {}
    void undo();
    void release();
};

class SynchronizedUndoOnlyAction : public UndoOnlyAction {
    UndoOnlyAction* m_realAction;
public:
    SynchronizedUndoOnlyAction(UndoOnlyAction* realAction) : m_realAction(realAction) {}
    virtual ~SynchronizedUndoOnlyAction() {}
    void undo();
};

class SynchronizedReleaseOnlyAction : public ReleaseOnlyAction {
    ReleaseOnlyAction* m_realAction;
public:
    SynchronizedReleaseOnlyAction(ReleaseOnlyAction* realAction) : m_realAction(realAction) {}
    virtual ~SynchronizedReleaseOnlyAction() {}
    void release();
};

class SynchronizedDummyUndoReleaseAction : public UndoReleaseAction {
public:
    SynchronizedDummyUndoReleaseAction() { }
    virtual ~SynchronizedDummyUndoReleaseAction() { }
    void undo() {
       countDown(false);
    }
    void release() {
       countDown(false);
    }
};

class SynchronizedDummyUndoOnlyAction : public UndoOnlyAction {
public:
    SynchronizedDummyUndoOnlyAction() { }
    virtual ~SynchronizedDummyUndoOnlyAction() { }
    void undo() {
       countDown(false);
    }
};

class SynchronizedDummyReleaseOnlyAction : public ReleaseOnlyAction {
public:
    SynchronizedDummyReleaseOnlyAction() { }
    virtual ~SynchronizedDummyReleaseOnlyAction() { }
    void release() {
       countDown(false);
    }
};

void UndoReleaseAction::countDown(bool flag) {
   assert(!SynchronizedThreadLock::isInSingleThreadMode());
   SynchronizedThreadLock::countDownGlobalTxnStartCount(flag);
}

void* UndoReleaseAction::operator new(std::size_t sz, UndoQuantum& uq) {
   return uq.allocateAction(sz);
}

void SynchronizedUndoReleaseAction::undo() {
   countDown(true);
   {    // For scoped ExecuteWithMPMemory local variable
      ExecuteWithMpMemory usingMpMemory;
      m_realAction->undo();
   }
   SynchronizedThreadLock::signalLowestSiteFinished();
}

void SynchronizedUndoReleaseAction::release() {
   countDown(true);
   {
      ExecuteWithMpMemory usingMpMemory;
      m_realAction->release();
   }
   SynchronizedThreadLock::signalLowestSiteFinished();
}

void SynchronizedUndoOnlyAction::undo() {
   countDown(true);
   {
      ExecuteWithMpMemory usingMpMemory;
      m_realAction->undo();
   }
   SynchronizedThreadLock::signalLowestSiteFinished();
}

void SynchronizedReleaseOnlyAction::release() {
   countDown(true);
   {
      ExecuteWithMpMemory usingMpMemory;
      m_realAction->release();
   }
   SynchronizedThreadLock::signalLowestSiteFinished();
}

std::unique_ptr<UndoReleaseAction> UndoReleaseAction::getSynchronizedUndoAction(UndoQuantum* currUQ) {
   return std::unique_ptr<UndoReleaseAction>(new (*currUQ) SynchronizedUndoReleaseAction(this));
}

std::unique_ptr<UndoReleaseAction> UndoReleaseAction::getDummySynchronizedUndoAction(UndoQuantum* currUQ) {
   return std::unique_ptr<UndoReleaseAction>(new (*currUQ) SynchronizedDummyUndoReleaseAction());
}

std::unique_ptr<UndoReleaseAction> UndoOnlyAction::getSynchronizedUndoAction(UndoQuantum* currUQ) {
   return std::unique_ptr<UndoReleaseAction>(new (*currUQ) SynchronizedUndoOnlyAction(this));
}

std::unique_ptr<UndoReleaseAction> UndoOnlyAction::getDummySynchronizedUndoAction(UndoQuantum* currUQ) {
   return std::unique_ptr<UndoReleaseAction>(new (*currUQ) SynchronizedDummyUndoOnlyAction());
}

std::unique_ptr<UndoReleaseAction> ReleaseOnlyAction::getSynchronizedUndoAction(UndoQuantum* currUQ) {
   return std::unique_ptr<UndoReleaseAction>(new (*currUQ) SynchronizedReleaseOnlyAction(this));
}

std::unique_ptr<UndoReleaseAction> ReleaseOnlyAction::getDummySynchronizedUndoAction(UndoQuantum* currUQ) {
   return std::unique_ptr<UndoReleaseAction>(new (*currUQ) SynchronizedDummyReleaseOnlyAction());
}

