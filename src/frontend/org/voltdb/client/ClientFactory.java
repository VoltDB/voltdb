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

package org.voltdb.client;

import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.VoltLogger;
import org.voltcore.network.ReverseDNSCache;
import org.voltcore.utils.EstTimeUpdater;

/**
 * Factory for constructing instances of the {@link Client} interface
 *
 */
public abstract class ClientFactory {

    static AtomicInteger ACTIVE_CLIENT_COUNT = new AtomicInteger(0);

    /**
     * <p>Create a {@link Client} with no connections. The Client will be optimized to send stored procedure invocations
     * that are 128 bytes in size. Authentication will use a blank username and password unless
     * you use the @deprecated createConnection methods.</p>
     *
     * @return Newly constructed {@link Client}
     */
    public static Client createClient() {
        if (ACTIVE_CLIENT_COUNT.incrementAndGet() == 1) {
            VoltLogger.startAsynchronousLogging();
            EstTimeUpdater.start();
            ReverseDNSCache.start();
        }
        return new ClientImpl(new ClientConfig());
    }

    /**
     * <p>Recommended method for creating a client. Using a ClientConfig object ensures
     * that a client application is isolated from changes to the configuration options.
     * Authentication credentials are provided at construction time with this method
     * instead of when invoking createConnection.</p>
     *
     * @param config A ClientConfig object specifying what type of client to create
     * @return A configured client
     */
    public static Client createClient(ClientConfig config) {
        if (ACTIVE_CLIENT_COUNT.incrementAndGet() == 1) {
            VoltLogger.startAsynchronousLogging();
            EstTimeUpdater.start();
            ReverseDNSCache.start();
        }
        return new ClientImpl(config);
    }

    public static void decreaseClientNum() throws InterruptedException {
        // the client is the last alive client. Before exit, close all the static resources and threads.
        int count = ACTIVE_CLIENT_COUNT.get();
        if (count <= 0) {
            return;
        }
        if (ACTIVE_CLIENT_COUNT.decrementAndGet() == 0) {
            //Shut down the logger.
            VoltLogger.shutdownAsynchronousLogging();
            //Estimate Time Updater stop updates.
            EstTimeUpdater.stop();
            //stop ReverseDNSCache.
            ReverseDNSCache.stop();
        }
        count = ACTIVE_CLIENT_COUNT.get();
        if (count < 0) {
            ACTIVE_CLIENT_COUNT.compareAndSet(count,0);
        }
    }

    public static void increaseClientCountToOne() {
        ACTIVE_CLIENT_COUNT.compareAndSet(0,1);
    }
}
