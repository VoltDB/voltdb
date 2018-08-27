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

#include "UndoQuantum.h"
#include "common/UndoReleaseAction.h"
#include "common/UndoQuantumReleaseInterest.h"

namespace voltdb {

Pool* UndoQuantum::undo() {
   for (auto iter = m_undoActions.rbegin(); iter != m_undoActions.rend(); ++iter) {    // TODO
      (*iter)->undo();
   }
   Pool * result = m_dataPool;
   delete this;
   // return the pool for recycling.
   return result;
}

Pool* UndoQuantum::release() {
   for (auto iter = m_undoActions.begin(); iter != m_undoActions.end(); ++iter) {
      (*iter)->release();
   }
   if (m_interests != nullptr) {
      for (int ii = 0; ii < m_numInterests; ii++) {
         m_interests[ii]->notifyQuantumRelease();
      }
      m_interests = nullptr;
   }
   Pool* result = m_dataPool;
   delete this;
   // return the pool for recycling.
   return result;
}

void UndoQuantum::registerUndoAction(std::unique_ptr<UndoReleaseAction>&& undoAction, UndoQuantumReleaseInterest *interest) {
   assert(undoAction);
   m_undoActions.emplace_back(std::move(undoAction));
   if (interest != NULL) {
      if (m_interests == NULL) {
         m_interests = reinterpret_cast<UndoQuantumReleaseInterest**>(m_dataPool->allocate(sizeof(void*) * 16));
         m_interestsCapacity = 16;
      }
      bool isDup = false;
      for (int ii = 0; ii < m_numInterests; ii++) {
         if (m_interests[ii] == interest) {
            isDup = true;
            break;
         }
      }
      if (!isDup) {
         if (m_numInterests == m_interestsCapacity) {
            // Don't need to explicitly free the old m_interests because all memory allocated by UndoQuantum
            // gets reset (by calling purge) to a clean state after the quantum completes
            UndoQuantumReleaseInterest **newStorage =
               reinterpret_cast<UndoQuantumReleaseInterest**>(m_dataPool->allocate(sizeof(void*) * m_interestsCapacity * 2));
            ::memcpy(newStorage, m_interests, sizeof(void*) * m_interestsCapacity);
            m_interests = newStorage;
            m_interestsCapacity *= 2;
         }
         m_interests[m_numInterests++] = interest;
      }
   }
}

}
