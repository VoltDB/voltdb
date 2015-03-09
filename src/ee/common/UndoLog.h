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

#ifndef UNDOLOG_H_
#define UNDOLOG_H_

#include <vector>
#include <deque>
#include <stdint.h>
#include <iostream>
#include <cassert>

#include "common/Pool.hpp"
#include "common/UndoQuantum.h"

namespace voltdb
{
    class UndoLog
    {
    public:
        UndoLog();
        virtual ~UndoLog();
        /**
         * Clean up all outstanding state in the UndoLog.  Essentially
         * contains all the work that should be performed by the
         * destructor.  Needed to work around a memory-free ordering
         * issue in VoltDBEngine's destructor.
         */
        void clear();

        inline UndoQuantum* generateUndoQuantum(int64_t nextUndoToken)
        {
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
            if (m_undoDataPools.size() == 0) {
                pool = new Pool(TEMP_POOL_CHUNK_SIZE, 1);
            } else {
                pool = m_undoDataPools.back();
                m_undoDataPools.pop_back();
            }
            assert(pool);
            UndoQuantum *undoQuantum = new (*pool) UndoQuantum(nextUndoToken, pool);
            m_undoQuantums.push_back(undoQuantum);
            return undoQuantum;
        }

        /*
         * Undo all undoable actions from the latest undo quantum back
         * until the undo quantum with the specified undo token.
         */
        inline void undo(const int64_t undoToken) {
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
            }

            m_lastUndoToken = undoToken - 1;
            while (m_undoQuantums.size() > 0) {
                UndoQuantum *undoQuantum = m_undoQuantums.back();
                const int64_t undoQuantumToken = undoQuantum->getUndoToken();
                if (undoQuantumToken < undoToken) {
                    return;
                }

                m_undoQuantums.pop_back();
                // Destroy the quantum, but retain its pool for reuse.
                Pool *pool = undoQuantum->undo();
                pool->purge();
                m_undoDataPools.push_back(pool);

                if(undoQuantumToken == undoToken) {
                    return;
                }
            }
        }

        /*
         * Release memory held by all undo quantums up to and
         * including the quantum with the specified token. It will be
         * impossible to undo these actions in the future.
         */
        inline void release(const int64_t undoToken) {
            //std::cout << "Releasing token " << undoToken
            //          << " lastUndo: " << m_lastUndoToken
            //          << " lastRelease: " << m_lastReleaseToken << std::endl;
            assert(m_lastReleaseToken < undoToken);
            m_lastReleaseToken = undoToken;
            while (m_undoQuantums.size() > 0) {
                UndoQuantum *undoQuantum = m_undoQuantums.front();
                const int64_t undoQuantumToken = undoQuantum->getUndoToken();
                if (undoQuantumToken > undoToken) {
                    return;
                }

                m_undoQuantums.pop_front();
                // Destroy the quantum, but retain its pool for reuse.
                Pool *pool = undoQuantum->release();
                pool->purge();
                m_undoDataPools.push_back(pool);
                if(undoQuantumToken == undoToken) {
                    return;
                }
            }
        }

        int64_t getSize() const
        {
            int64_t total = 0;
            for (int i = 0; i < m_undoDataPools.size(); i++)
            {
                total += m_undoDataPools[i]->getAllocatedMemory();
            }
            for (int i = 0; i < m_undoQuantums.size(); i++)
            {
                total += m_undoQuantums[i]->getAllocatedMemory();
            }
            return total;
        }

    private:
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
        int64_t m_lastUndoToken;

        // m_lastReleaseToken is the largest token that definitely
        // doesn't exist; any smaller value has already been released,
        // any larger value might exist (gaps are possible)
        int64_t m_lastReleaseToken;

        std::vector<Pool*> m_undoDataPools;
        std::deque<UndoQuantum*> m_undoQuantums;
    };
}
#endif /* UNDOLOG_H_ */
