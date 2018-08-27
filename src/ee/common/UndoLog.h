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

#include <vector>
#include <deque>
#include <numeric>
#include <cstdint>
#include <iostream>
#include <cassert>
#include <chrono> // For measuring the execution time of each fragment.

#include "common/Pool.hpp"
#include "common/UndoQuantum.h"

namespace voltdb {
    class UndoLog
    {
       static const size_t MAX_CACHED_POOLS = 192;
        // These two values serve no real purpose except to provide
        // the capability to assert various properties about the undo tokens
        // handed to the UndoLog.  Currently, this makes the following
        // assumptions about how the Java side is managing undo tokens:
        //
        // 1. Java is generating monotonically increasing undo tokens.
        // There may be gaps, but every new token to generateUndoQuantum is
        // larger than every other token previously seen by generateUndoQuantum
        //
        // 2. Right now, the ExecutionSite _always_ releases the
        // largest token generated during a transaction at the end of the
        // transaction, even if the entire transaction was rolled back.  This
        // means that release may get called even if there are no undo quanta
        // present.

        // m_lastUndoToken is the largest token that could possibly be called
        // for real undo; any larger token is either undone or has
        // never existed
        int64_t m_lastUndoToken = INT64_MIN;

        // m_lastReleaseToken is the largest token that definitely
        // doesn't exist; any smaller value has already been released,
        // any larger value might exist (gaps are possible)
        int64_t m_lastReleaseToken = INT64_MIN;

        bool m_undoLogForLowestSite = false;
        std::vector<Pool*> m_undoDataPools;
        std::deque<UndoQuantum*> m_undoQuantums;
    public:
        virtual ~UndoLog() {
           clear();
        }
        /**
         * Clean up all outstanding state in the UndoLog.  Essentially
         * contains all the work that should be performed by the
         * destructor.  Needed to work around a memory-free ordering
         * issue in VoltDBEngine's destructor.
         */
        void clear() throw() {
           if (!m_undoQuantums.empty()) {
              release(m_lastUndoToken);
           }
           std::for_each(m_undoDataPools.begin(), m_undoDataPools.end(), [](Pool* i) {delete i;});
           m_undoDataPools.clear();
           m_undoQuantums.clear();
        }

        void setUndoLogForLowestSite() { m_undoLogForLowestSite = true; }

        /**
         * Retrieve the last undo quantum that caller can change.
         */
        UndoQuantum*& getLastUndoQuantum() {
           assert(! m_undoQuantums.empty());
           return m_undoQuantums.back();
        }

        UndoQuantum* generateUndoQuantum(int64_t nextUndoToken) {
            //std::cout << "Generating token " << nextUndoToken
            //          << " lastUndo: " << m_lastUndoToken
            //          << " lastRelease: " << m_lastReleaseToken << std::endl;
            // Since ExecutionSite is using monotonically increasing
            // token values, every new quanta we're asked to generate should be
            // larger than any token value we've seen before
            assert(nextUndoToken > m_lastUndoToken);
            assert(nextUndoToken > m_lastReleaseToken);
            m_lastUndoToken = nextUndoToken;
            Pool *pool = NULL;
            if (m_undoDataPools.empty()) {
                pool = new Pool(TEMP_POOL_CHUNK_SIZE, 1);
            } else {
                pool = m_undoDataPools.back();
                m_undoDataPools.pop_back();
            }
            assert(pool);
            UndoQuantum *undoQuantum = new (*pool) UndoQuantum(nextUndoToken, pool, m_undoLogForLowestSite);
            m_undoQuantums.push_back(undoQuantum);
            return undoQuantum;
        }

        /*
         * Undo all undoable actions from the latest undo quantum back
         * until the undo quantum with the specified undo token.
         */
        void undo(const int64_t undoToken) {
            //std::cout << "Undoing token " << undoToken
            //          << " lastUndo: " << m_lastUndoToken
            //          << " lastRelease: " << m_lastReleaseToken << std::endl;
            // This ensures that undo is only ever called after

            // commenting out this assertion because it isn't valid (hugg 3/29/13)
            // if you roll back a proc that hasn't done any work, you can run
            // into this situation. Needs a better fix than this.
            // assert(m_lastReleaseToken < m_lastUndoToken);

            // This ensures that we don't attempt to undo something in
            // the distant past.  In some cases ExecutionSite may hand
            // us the largest token value that definitely doesn't
            // exist; this will just result in all undo quanta being undone.
            assert(undoToken >= m_lastReleaseToken);

            if (undoToken > m_lastUndoToken) {
                // a procedure may abort before it sends work to the EE
                // (informing the EE of its undo token. For example, it
                // may have invalid parameter values or, possibly, aborts
                // in user java code before executing any SQL.  Just
                // return. There is no work to do here.
                return;
            } else {
               m_lastUndoToken = undoToken - 1;
               while (! m_undoQuantums.empty()) {
                  UndoQuantum *undoQuantum = m_undoQuantums.back();
                  const int64_t undoQuantumToken = undoQuantum->getUndoToken();
                  if (undoQuantumToken < undoToken) {
                     break;
                  }
                  m_undoQuantums.pop_back();
                  // Destroy the quantum, but possibly retain its pool for reuse.
                  Pool *pool = undoQuantum->undo();
                  pool->purge();
                  if (m_undoDataPools.size() < MAX_CACHED_POOLS) {
                     m_undoDataPools.push_back(pool);
                  } else {
                     delete pool;
                  }
                  if(undoQuantumToken == undoToken) {
                     break;
                  }
               }
            }
        }

        /*
         * Release memory held by all undo quantums up to and
         * including the quantum with the specified token. It will be
         * impossible to undo these actions in the future.
         */
        void release(const int64_t undoToken) {
            //std::cout << "Releasing token " << undoToken
            //          << " lastUndo: " << m_lastUndoToken
            //          << " lastRelease: " << m_lastReleaseToken << std::endl;
            assert(m_lastReleaseToken < undoToken);
            auto const startTime = std::chrono::high_resolution_clock::now();
            m_lastReleaseToken = undoToken;
            while (! m_undoQuantums.empty()) {
                UndoQuantum *undoQuantum = m_undoQuantums.front();
                const int64_t undoQuantumToken = undoQuantum->getUndoToken();
                if (undoQuantumToken > undoToken) {
                   goto CLEAN;
                }

                m_undoQuantums.pop_front();
                // Destroy the quantum, but possibly retain its pool for reuse.
                Pool *pool = undoQuantum->release();
                pool->purge();
                if (m_undoDataPools.size() < MAX_CACHED_POOLS) {
                    m_undoDataPools.push_back(pool);
                } else {
                    delete pool; pool = NULL;
                }
                if(undoQuantumToken == undoToken) {
                   goto CLEAN;
                }
            }
CLEAN:
               fprintf(stderr, "%ld us\n",
                     std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - startTime)
                     .count()/1000);
        }

        int64_t getSize() const {
            return
               std::accumulate(m_undoDataPools.cbegin(), m_undoDataPools.cend(), 0lu,
                  [](size_t acc, Pool const* cur){ return acc + cur->getAllocatedMemory(); }) +
               std::accumulate(m_undoQuantums.cbegin(), m_undoQuantums.cend(), 0lu,
                     [](size_t acc, UndoQuantum const* cur) { return acc + cur->getAllocatedMemory(); });
        }

    };
}

