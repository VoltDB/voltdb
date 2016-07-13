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

#ifndef SYNCHRONIZEDTHREADLOCK_H_
#define SYNCHRONIZEDTHREADLOCK_H_

//#include "boost/scoped_ptr.hpp"
//#include "boost/unordered_map.hpp"

#include <cassert>
#include <map>
#include <stack>
#include <string>
#include <vector>
#include <pthread.h>

namespace voltdb {

extern pthread_mutex_t sharedEngineMutex;
extern pthread_cond_t sharedEngineCondition;
extern std::atomic<int32_t> globalTxnStartCountdownLatch;
extern int32_t globalTxnEndCountdownLatch;
extern int32_t SITES_PER_HOST;

class SynchronizedThreadLock {
public:
    static void init(int32_t sitesPerHost);
    /**
     * Cross-site synchronization functions
     */
    static bool countDownGlobalTxnStartCount();
    static void signalLastSiteFinished();
    static void waitForLastSiteFinished();
};

}


#endif
