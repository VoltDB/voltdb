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


#pragma once
#include <cstdlib>
#include "common/UndoQuantum.h"

namespace voltdb {
   class UndoQuantum;
   /*
    * Abstract base class for all classes generated to undo changes to the system.
    * Always memory-managed by and registered with an undo quantum.
    */
   class UndoReleaseAction {
      protected:
         static void countDown(bool);
      public:
         void* operator new(std::size_t sz, UndoQuantum& uq);
         void operator delete(void*, UndoQuantum&) { /* emergency deallocator does nothing */ }
         void operator delete(void*) { /* every-day deallocator does nothing -- lets the pool cope */ }

         UndoReleaseAction() {}
         virtual ~UndoReleaseAction() {}

         /*
          * Undo whatever this undo action was created to undo
          */
         virtual void undo() {}

         /*
          * Release any resources held by the undo action. It will not need to be undone in the future.
          */
         virtual void release() {}

         /*
          * Generate a synchronized Version of UndoAction
          */
         virtual std::unique_ptr<UndoReleaseAction> getSynchronizedUndoAction(UndoQuantum* currUQ);
         virtual std::unique_ptr<UndoReleaseAction> getDummySynchronizedUndoAction(UndoQuantum* currUQ);
   };

   class UndoOnlyAction : public UndoReleaseAction {
      public:
         UndoOnlyAction() {}
         virtual ~UndoOnlyAction() {}
         virtual std::unique_ptr<UndoReleaseAction> getSynchronizedUndoAction(UndoQuantum* currUQ);
         virtual std::unique_ptr<UndoReleaseAction> getDummySynchronizedUndoAction(UndoQuantum* currUQ);
   };

   class ReleaseOnlyAction : public UndoReleaseAction {
      public:
         ReleaseOnlyAction() {}
         virtual ~ReleaseOnlyAction() {}
         virtual std::unique_ptr<UndoReleaseAction> getSynchronizedUndoAction(UndoQuantum* currUQ);
         virtual std::unique_ptr<UndoReleaseAction> getDummySynchronizedUndoAction(UndoQuantum* currUQ);
   };
}
