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
     * @return Newly constructed {@link Client}
     * @see Client
     */
    public static Client createClient(int expectedOutgoingMessageSize, int maxArenaSizes[]) {
        return new ClientImpl(expectedOutgoingMessageSize, maxArenaSizes);
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
