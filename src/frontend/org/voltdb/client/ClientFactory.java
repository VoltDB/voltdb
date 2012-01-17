/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
     * Create a {@link Client} with no connections. The Client will be optimized to send stored procedure invocations
     * that are 128 bytes in size. Authentication will use a blank username and password unless
     * you use the @deprecated createConnection methods.
     * @return Newly constructed {@link Client}
     */
    public static Client createClient() {
        return new ClientImpl(new ClientConfig());
    }

    /**
     * Recommended method for creating a client. Using a ClientConfig object ensures
     * that a client application is isolated from changes to the configuration options.
     * Authentication credentials are provided at construction time with this method
     * instead of when invoking createConnection.
     * @param config A ClientConfig object specifying what type of client to create
     * @return A configured client
     */
    public static Client createClient(ClientConfig config) {
        return new ClientImpl(config);
    }
}
