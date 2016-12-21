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

import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;
import org.voltcore.utils.CoreUtils;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.voltcore.logging.VoltLogger;

/**
 * A general service that accepts callables.  Used to encrypt and decrypt byte buffers
 * with ssl.
 */
public class SSLEncryptionService {

    private static SSLEncryptionService m_self;
    private static final VoltLogger networkLog = new VoltLogger("NETWORK");

    private static final ListeningExecutorService m_EncEs = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(Math.max(2, CoreUtils.availableProcessors()/2),
                    CoreUtils.getThreadFactory("SSL encryption thread"))
    );

    private static final ListeningExecutorService m_DecEs = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(Math.max(2, CoreUtils.availableProcessors()/2),
                    CoreUtils.getThreadFactory("SSL decryption thread"))
    );

    public static SSLEncryptionService initialize(int nThreads) {
        m_self = new SSLEncryptionService(nThreads);
        return m_self;
    }

    public static SSLEncryptionService instance() {
        return m_self;
    }

    public SSLEncryptionService(int nThreads) {
    }

    public void shutdown() throws InterruptedException {
        networkLog.info("Shutting down Encryption and Decryption services.");
        if (m_EncEs != null) {
            m_EncEs.shutdown();
            m_EncEs.awaitTermination(365, TimeUnit.DAYS);
        }
        if (m_DecEs != null) {
            m_DecEs.shutdown();
            m_DecEs.awaitTermination(365, TimeUnit.DAYS);
        }
        networkLog.info("Encryption and Decryption services successfully shutdown.");
    }

    public ListenableFuture<?> submitForEncryption(Runnable task) {
        return m_EncEs.submit(task);
    }

    public ListenableFuture<?> submitForDecryption(Runnable task) {
        return m_DecEs.submit(task);
    }
}
