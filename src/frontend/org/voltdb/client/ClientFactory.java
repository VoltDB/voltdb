/* This file is part of VoltDB.
 * Copyright (C) 2008-2021 VoltDB Inc.
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

import org.voltcore.network.ReverseDNSCache;
import org.voltcore.utils.EstTimeUpdater;

/**
 * Factory for constructing instances of the {@link Client} interface
 *
 */
public abstract class ClientFactory {

    static int m_activeClientCount = 0;
    static boolean m_preserveResources = false;

    /**
     * Create a {@link Client} with no connections and all default options.
     * Authentication will use a blank username and password.
     *
     * @return Newly constructed {@link Client}
     */
    public static Client createClient() {
        return createClient(new ClientConfig());
    }

    /**
     * Recommended method for creating a {@link Client}.
     * <p>
     * Using a {@link ClientConfig} object ensures that a client application
     * is isolated from changes to the configuration options. Authentication
     * credentials are provided via the configuration object.</p>
     *
     * @param config A {@link ClientConfig} object
     * @return A configured {@link Client}
     */
    public static Client createClient(ClientConfig config) {
        start();
        return new ClientImpl(config);
    }

    /**
     * Create a "version 2" client, {@link Client2}.
     * <p>
     * This call takes a {@link Client2Config} argument, which
     * distinguishes it from {@link #createClient(ClientConfig)}.
     * <p>
     * All client options, including authentication information, are
     * provided via the {@link Client2Config} object.
     *
     * @param config A {@link Client2Config} object
     * @return A configured {@link Client2}
     */
    public static Client2 createClient(Client2Config config) {
        start();
        return new Client2Impl(config);
    }

    private static synchronized void start() {
        if (m_activeClientCount++ == 0 && !m_preserveResources) {
            EstTimeUpdater.start();
            ReverseDNSCache.start();
        }
    }

    /**
     * Internally used by the VoltDB server during initialization.
     * <p>
     * This method is intended to ensure that the resources needed to create clients
     * are always initialized and won't be released when the active client count goes to zero.
     */
    public static synchronized void preserveResources() {
        m_preserveResources = true;
        EstTimeUpdater.start();
        ReverseDNSCache.start();
    }

    /**
     * Internal client implementation support.
     * <p>
     * If the client is the last alive client, then close all the static
     * resources, and stop threads created by 'start'.
     */
    static synchronized void decreaseClientNum() throws InterruptedException {
        if (m_activeClientCount > 0) {
            if (--m_activeClientCount == 0 && !m_preserveResources) {
                EstTimeUpdater.stop();
                ReverseDNSCache.stop();
            }
        }
    }

}
