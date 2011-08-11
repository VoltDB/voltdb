/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.clientutils;

import org.voltdb.client.*;
import java.util.HashMap;

public class ClientConnection
{
    protected final String Key;
    public final Client Client;
    protected short Users;
    protected ClientConnection(String clientConnectionKey, String[] servers, int port, String user, String password, boolean isHeavyWeight, int maxOutstandingTxns) throws Exception
    {
        this.Key = clientConnectionKey;

        // Create configuration
        final ClientConfig config = new ClientConfig(user, password);
        config.setHeavyweight(isHeavyWeight);
        if (maxOutstandingTxns > 0)
            config.setMaxOutstandingTxns(maxOutstandingTxns);

        // Create client
        final Client client = ClientFactory.createClient(config);

        // Create ClientConnections
        for (String server : servers)
            if (server.trim().length() > 0)
                client.createConnection(server.trim(), port);

        this.Client = client;
        this.Users = 0;
    }
    protected ClientConnection use()
    {
        this.Users++;
        return this;
    }
    protected void dispose()
    {
        this.Users--;
        if (this.Users == 0)
        {
            try { this.Client.close(); } catch(Exception x) {}
        }
    }
}

