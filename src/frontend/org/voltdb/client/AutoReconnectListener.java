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
package org.voltdb.client;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltdb.utils.CSVDataLoader;

public class AutoReconnectListener extends ClientStatusListenerExt {
    private static final long MIN_RETRY_INTERVAL = 1000;
    private static final long MAX_RETRY_INTERVAL = 8000;

    private final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "Retry Connection");
            thread.setDaemon(true);
            return thread;
        }
    });

    private static final VoltLogger LOG = new VoltLogger("HOST");

    private CSVDataLoader m_dataLoader;
    private Client m_client;

    @Override
    public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
        LOG.warn(String.format("Connection to VoltDB node at: %s:%d was lost.", hostname, port));
        executor.execute(new Runnable() {
            @Override
            public void run() {
                retryConnection(hostname, port);
            }
        });
    }

    public void setLoader(CSVDataLoader loader) {
        m_dataLoader = loader;
    }

    public void setClient(Client client) {
        m_client = client;
    }

    private void retryConnection(String hostname, int port) {
        long sleep = MIN_RETRY_INTERVAL;
        while (true) {
            try {
                m_client.createConnection(hostname, port);
                LOG.info(String.format("Connected to VoltDB node at %s:%d.", hostname, port));
                m_dataLoader.resumeLoading();
                break;
            } catch (Exception e) {
                LOG.warn(String.format("Connection to VoltDB node at %s:%d failed - retrying in %d second(s).",
                        hostname, port, TimeUnit.MILLISECONDS.toSeconds(sleep)));
                try {
                    Thread.sleep(sleep);
                } catch (Exception ignored) {
                }
                if (sleep < MAX_RETRY_INTERVAL) {
                    sleep = Math.min(sleep * 2, MAX_RETRY_INTERVAL);
                }
            }
        }
    }
}
