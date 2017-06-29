/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

#include "common/debuglog.h"

namespace voltdb {

// Initialized when executor context is created.
pthread_mutex_t sharedEngineMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t sharedEngineCondition;
pthread_cond_t wakeLowestEngineCondition;
int32_t globalTxnStartCountdownLatch = 0;
int32_t SITES_PER_HOST = -1;

void SynchronizedThreadLock::init(int32_t sitesPerHost)
{
    if (SITES_PER_HOST == 0) {
        SITES_PER_HOST = sitesPerHost;
        globalTxnStartCountdownLatch = SITES_PER_HOST;
    }
}

bool SynchronizedThreadLock::countDownGlobalTxnStartCount(bool lowestSite)
{
    assert(globalTxnStartCountdownLatch > 0);
    if (lowestSite) {
        pthread_mutex_lock(&sharedEngineMutex);
        if (--globalTxnStartCountdownLatch != 0) {
            pthread_cond_wait(&wakeLowestEngineCondition, &sharedEngineMutex);
        }
        pthread_mutex_unlock(&sharedEngineMutex);
        return true;
    }
    else {
        pthread_mutex_lock(&sharedEngineMutex);
        if (--globalTxnStartCountdownLatch == 0) {
            pthread_cond_broadcast(&wakeLowestEngineCondition);
        }
        pthread_cond_wait(&sharedEngineCondition, &sharedEngineMutex);
        pthread_mutex_unlock(&sharedEngineMutex);
        return false;
    }
}

void SynchronizedThreadLock::signalLowestSiteFinished()
{
    pthread_mutex_lock(&sharedEngineMutex);
    globalTxnStartCountdownLatch = SITES_PER_HOST;
    pthread_cond_broadcast(&sharedEngineCondition);
    pthread_mutex_unlock(&sharedEngineMutex);
}
}
