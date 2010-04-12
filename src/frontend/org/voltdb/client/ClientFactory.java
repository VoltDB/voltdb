/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.client;

/**
 * Factory for constructing instances of the {@link Client} interface
 *
 */
public abstract class ClientFactory {

    /**
     * Create a {@link Client} with no connections that is optimized to send stored procedure invocations
     * that serialize to the specified size. Also provides limits on what memory pool arenas should
     * be allowed to grow to
     * @param expectedOutgoingMessageSize Expected serialized size of most stored procedure invocations
     * @param maxArenaSizes Maximum size each arena will be allowed to grow to. Can be <code>null</code>
     * @param heavyweight If set to true the Client API will use multiple threads in order to be able
     * to saturate bonded gigabit connections. Only set to true if you have at least 2 bonded links
     * and intend to saturate them using this client instance. When set to false it can still saturate a gigabit
     * connection. Arena sizes are ignored when heavyweight is set. This is ignored on systems with < 4 cores.
     * @param statsSettings Settings for uploading statistical information via JDBC. Can be null in which
     * case stats will not be uploaded.
     * @return Newly constructed {@link Client}
     * @see Client
     */
    public static Client createClient(
            int expectedOutgoingMessageSize,
            int maxArenaSizes[],
            boolean heavyweight,
            StatsUploaderSettings statsSettings) {
        final int cores = Runtime.getRuntime().availableProcessors();
        return new ClientImpl(
                expectedOutgoingMessageSize,
                maxArenaSizes,
                cores > 4 ? heavyweight : false,
                statsSettings);
    }

    /**
     * Create a {@link Client} with no connections. The Client will be optimized to send stored procedure invocations
     * that are 128 bytes in size.
     * @return Newly constructed {@link Client}
     */
    public static Client createClient() {
        return new ClientImpl();
    }
}
