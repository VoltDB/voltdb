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

package org.voltcore.utils.ssl;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;
import org.voltcore.utils.CoreUtils;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.VoltLogger;

/**
 * A general service that accepts callables.  Used to encrypt and decrypt byte buffers
 * with ssl.
 */
public class SSLEncryptionService {

    private static SSLEncryptionService m_server_self = new SSLEncryptionService(true);

    private final AtomicBoolean m_isShutdown = new AtomicBoolean(false);

    private final VoltLogger networkLog = new VoltLogger("NETWORK");

    private final ListeningExecutorService m_encEs;

    private final ListeningExecutorService m_decEs;

    public static SSLEncryptionService serverInstance() {
        return m_server_self;
    }

    public SSLEncryptionService(boolean serverConfig) {
        if (serverConfig) {
            m_encEs = MoreExecutors.listeningDecorator(
                    Executors.newFixedThreadPool(Math.max(2, CoreUtils.availableProcessors()/2),
                            CoreUtils.getThreadFactory("SSL encryption thread")));
            m_decEs = MoreExecutors.listeningDecorator(
                    Executors.newFixedThreadPool(Math.max(2, CoreUtils.availableProcessors()/2),
                            CoreUtils.getThreadFactory("SSL decryption thread")));
        } else {
            // client config
            m_encEs = MoreExecutors.listeningDecorator(
                    Executors.newFixedThreadPool(1,
                            CoreUtils.getThreadFactory("SSL encryption thread")));
            m_decEs = MoreExecutors.listeningDecorator(
                    Executors.newFixedThreadPool(1,
                            CoreUtils.getThreadFactory("SSL decryption thread")));
        }
    }

    public void startup() {
        m_isShutdown.set(false);
    }

    public void shutdown() throws InterruptedException {

        m_isShutdown.set(true);
        networkLog.debug("Shutting down Encryption and Decryption services.");
        if (m_encEs != null) {
            m_encEs.shutdown();
            m_encEs.awaitTermination(365, TimeUnit.DAYS);
        }
        if (m_decEs != null) {
            m_decEs.shutdown();
            m_decEs.awaitTermination(365, TimeUnit.DAYS);
        }
        networkLog.debug("Encryption and Decryption services successfully shutdown.");
    }

    public ListenableFuture<?> submitForEncryption(Runnable task) {
        if (m_isShutdown.get()) {
            return null;
        }
        return m_encEs.submit(task);
    }

    public ListenableFuture<?> submitForDecryption(Runnable task) {
        if (m_isShutdown.get()) {
            return null;
        }
        return m_decEs.submit(task);
    }
}
