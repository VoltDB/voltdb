/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "common/executorcontext.hpp"
#include <pthread.h>

using namespace std;

namespace voltdb {

static pthread_key_t m_key;
static pthread_once_t m_keyOnce = PTHREAD_ONCE_INIT;

static void createThreadLocalKey() {
    (void)pthread_key_create( &m_key, NULL);
}

ExecutorContext::ExecutorContext(CatalogId siteId,
                CatalogId partitionId,
                UndoQuantum *undoQuantum,
                Topend* topend,
                bool exportEnabled,
                int64_t epoch,
                std::string hostname,
                CatalogId hostId) :
    m_topEnd(topend), m_undoQuantum(undoQuantum),
    m_txnId(0),
    m_siteId(siteId), m_partitionId(partitionId),
    m_hostname(hostname), m_hostId(hostId),
    m_exportEnabled(exportEnabled), m_epoch(epoch)
{
    (void)pthread_once(&m_keyOnce, createThreadLocalKey);
    pthread_setspecific( m_key, static_cast<const void *>(this));
    m_lastCommittedTxnId = 0;
}

ExecutorContext* ExecutorContext::getExecutorContext() {
    return static_cast<ExecutorContext*>(pthread_getspecific(m_key));
}
}

