/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

#include <common/UndoLog.h>
#include "common/ExecuteWithMpMemory.h"

namespace voltdb {

std::atomic<int32_t> UndoQuantumReleaseInterest::s_uniqueTableId(0);
UndoLog::UndoLog()
  : m_lastUndoToken(INT64_MIN), m_lastReleaseToken(INT64_MIN)
{
}

void UndoLog::clear()
{
    if (! m_undoQuantums.empty()) {
        release(m_lastUndoToken);
    }
    for (auto i = m_undoDataPools.begin(); i != m_undoDataPools.end(); i++) {
        delete *i;
    }
    m_undoDataPools.clear();
    m_undoQuantums.clear();
}

UndoLog::~UndoLog()
{
    clear();
}

void UndoLog::release(const int64_t undoToken) {
    //std::cout << "Releasing token " << undoToken
    //          << " lastUndo: " << m_lastUndoToken
    //          << " lastRelease: " << m_lastReleaseToken << std::endl;
    vassert(m_lastReleaseToken < undoToken);
    m_lastReleaseToken = undoToken;
    std::set<UndoQuantumReleaseInterest*, ptr_less<UndoQuantumReleaseInterest>> releaseInterests{};
    while (!m_undoQuantums.empty()) {
        UndoQuantum *undoQuantum = m_undoQuantums.front();
        const int64_t undoQuantumToken = undoQuantum->getUndoToken();
        std::list<UndoQuantumReleaseInterest*>& canceledInterests = undoQuantum->getUndoQuantumCanceledInterests();
        std::list<UndoQuantumReleaseInterest*>& releasedInterests = undoQuantum->getUndoQuantumReleasedInterests();
        BOOST_FOREACH (auto interest, canceledInterests) {
            releaseInterests.erase(interest);
        }
        BOOST_FOREACH (auto interest, releasedInterests) {
            releaseInterests.insert(interest);
        }
        if (undoQuantumToken > undoToken) {
            break;
        }

        m_undoQuantums.pop_front();
        // Destroy the quantum, but possibly retain its pool for reuse.
        Pool *pool = UndoQuantum::release(std::move(*undoQuantum));
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
    BOOST_FOREACH (auto interest, releaseInterests) {
        ConditionalSynchronizedExecuteWithMpMemory possiblySynchronizedUseMpMemory
                (false, //interest->isReplicatedTable(),
                        ExecutorContext::getEngine()->isLowestSite(), []() {});
        if (possiblySynchronizedUseMpMemory.okToExecute()) {

        interest->finalizeRelease();
        }
    }
}

}
