/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
#include "LogManager.h"

/**
 * Thread local key for storing references to thread specific log managers.
 */
static pthread_key_t m_key;
static pthread_once_t m_keyOnce = PTHREAD_ONCE_INIT;

namespace voltdb {

static void createThreadLocalKey() {
    (void)pthread_key_create( &m_key, NULL);
}

LogManager* LogManager::getThreadLogManager() {
    return static_cast<LogManager*>(pthread_getspecific(m_key));
}

/*
template <>
const Logger<LOGGERID_SQL>* LogManager::getThreadLogger() {
    return static_cast<LogManager*>(pthread_getspecific(m_key))->getLogger<LOGGERID_SQL>();
}

template <>
const Logger<LOGGERID_HOST>* LogManager::getThreadLogger() {
    return static_cast<LogManager*>(pthread_getspecific(m_key))->getLogger<LOGGERID_HOST>();
}*/

/**
 * Constructor that initializes all the loggers with the specified proxy and
 * sets up a thread local containing a reference to itself.
 * @param proxy The LogProxy that all the loggers should use
 */
LogManager::LogManager(LogProxy *proxy) :  m_proxy(proxy), m_sqlLogger(proxy, LOGGERID_SQL), m_hostLogger(proxy, LOGGERID_HOST) {
    (void)pthread_once(&m_keyOnce, createThreadLocalKey);
    pthread_setspecific( m_key, static_cast<const void *>(this));
}

}

