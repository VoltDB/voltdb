/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

#include "SynchronizedThreadLock.h"
#include "executorcontext.hpp"

namespace voltdb {

pthread_mutex_t sharedEngineMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t sharedEngineCondition;
std::atomic<int32_t> globalTxnStartCountdownLatch(0);
int32_t globalTxnEndCountdownLatch = 0;
int32_t SITES_PER_HOST = -1;

void SynchronizedThreadLock::init(int32_t sitesPerHost)
{
    if (SITES_PER_HOST == 0) {
        SITES_PER_HOST = sitesPerHost;
        globalTxnStartCountdownLatch = SITES_PER_HOST;
    }
}

bool SynchronizedThreadLock::countDownGlobalTxnStartCount()
{
    assert(globalTxnStartCountdownLatch > 0);
    return --globalTxnStartCountdownLatch == 0;
}

void SynchronizedThreadLock::signalLastSiteFinished()
{
    pthread_mutex_lock(&sharedEngineMutex);
    globalTxnEndCountdownLatch++;
    while (globalTxnEndCountdownLatch != SITES_PER_HOST) {
        pthread_mutex_unlock(&sharedEngineMutex);
#ifdef __linux__
        pthread_yield();
#else
        sched_yield();
#endif
        pthread_mutex_lock(&sharedEngineMutex);
    }
    // We now know all other threads are waiting to be signaled
    globalTxnEndCountdownLatch = 0;
    globalTxnStartCountdownLatch = SITES_PER_HOST;
    pthread_cond_broadcast(&sharedEngineCondition);
    pthread_mutex_unlock(&sharedEngineMutex);
}

void SynchronizedThreadLock::waitForLastSiteFinished()
{
    pthread_mutex_lock(&sharedEngineMutex);
    globalTxnEndCountdownLatch++;
    pthread_cond_wait(&sharedEngineCondition, &sharedEngineMutex);
    pthread_mutex_unlock(&sharedEngineMutex);
}
}
