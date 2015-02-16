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
#include "common/executorcontext.hpp"

#include "common/debuglog.h"

#include <pthread.h>

using namespace std;

namespace voltdb {

static pthread_key_t static_key;
static pthread_once_t static_keyOnce = PTHREAD_ONCE_INIT;

static void createThreadLocalKey() {
    (void)pthread_key_create( &static_key, NULL);
}

ExecutorContext::ExecutorContext(int64_t siteId,
                CatalogId partitionId,
                UndoQuantum *undoQuantum,
                Topend* topend,
                Pool* tempStringPool,
                VoltDBEngine* engine,
                std::string hostname,
                CatalogId hostId,
                DRTupleStream *drStream) :
    m_topEnd(topend),
    m_tempStringPool(tempStringPool),
    m_undoQuantum(undoQuantum),
    m_drStream(drStream),
    m_engine(engine),
    m_txnId(0),
    m_spHandle(0),
    m_lastCommittedSpHandle(0),
    m_siteId(siteId),
    m_partitionId(partitionId),
    m_hostname(hostname),
    m_hostId(hostId),
    m_epoch(0) // set later
{
    (void)pthread_once(&static_keyOnce, createThreadLocalKey);
    bindToThread();
}

void ExecutorContext::bindToThread()
{
    // There can be only one (per thread).
    assert(pthread_getspecific( static_key) == NULL);
    pthread_setspecific( static_key, this);
    VOLT_DEBUG("Installing EC(%ld)", (long)this);
}

ExecutorContext::~ExecutorContext() {
    // currently does not own any of its pointers

    // There can be only one (per thread).
    assert(pthread_getspecific( static_key) == this);
    // ... or none, now that the one is going away.
    VOLT_DEBUG("De-installing EC(%ld)", (long)this);

    pthread_setspecific( static_key, NULL);
}

ExecutorContext* ExecutorContext::getExecutorContext() {
    (void)pthread_once(&static_keyOnce, createThreadLocalKey);
    return static_cast<ExecutorContext*>(pthread_getspecific( static_key));
}
}

