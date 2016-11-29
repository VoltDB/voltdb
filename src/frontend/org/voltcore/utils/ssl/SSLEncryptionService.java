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

package org.voltcore.utils.ssl;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A general service that accepts callables.  Used to encrypt and decrypt byte buffers
 * with ssl.
 */
public class SSLEncryptionService {

    private static SSLEncryptionService m_self;
    private final ExecutorService m_es;


    public static SSLEncryptionService initialize(int nThreads) {
        m_self = new SSLEncryptionService(nThreads);
        return m_self;
    }

    public static SSLEncryptionService instance() {
        return m_self;
    }

    public SSLEncryptionService(int nThreads) {
        this.m_es = Executors.newFixedThreadPool(nThreads);
    }

    public void shutdown() throws InterruptedException {
        if (m_es != null) {
            m_es.shutdown();
            m_es.awaitTermination(120, TimeUnit.SECONDS);
        }
    }

    public <T> Future<T> submit(Callable<T> task) {
        return m_es.submit(task);
    }
}
