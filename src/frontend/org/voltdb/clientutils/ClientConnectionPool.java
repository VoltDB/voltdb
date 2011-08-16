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

import org.voltdb.client.Client;
import java.util.HashMap;

public class ClientConnectionPool
{
    private static final short MAX_USERS_PER_CLIENT = 50;
    private static final HashMap<String,ClientConnection> ClientConnections = new HashMap<String,ClientConnection>();
    public static synchronized ClientConnection get(String[] servers, int port) throws Exception
    {
        return get(servers, port, "", "", false, 0);
    }
    public static synchronized ClientConnection get(String[] servers, int port, String user, String password, boolean isHeavyWeight, int maxOutstandingTxns) throws Exception
    {
        String clientConnectionKey = user + ":" + password + "@";
        for(int i=0;i<servers.length;i++)
            clientConnectionKey += servers[i] + ",";
        clientConnectionKey += ":" + Integer.toString(port) + "{" + Boolean.toString(isHeavyWeight) + ":" + Integer.toString(maxOutstandingTxns) + "}";
        int cnt = 1;
        while(ClientConnections.containsKey(clientConnectionKey))
        {
            if (ClientConnections.get(clientConnectionKey).Users >= MAX_USERS_PER_CLIENT)
                clientConnectionKey += "::" + cnt++;
            else
                break;
        }
        if (!ClientConnections.containsKey(clientConnectionKey))
            ClientConnections.put(clientConnectionKey, new ClientConnection(clientConnectionKey, servers, port, user, password, isHeavyWeight, maxOutstandingTxns));
        return ClientConnections.get(clientConnectionKey).use();
    }
    public static synchronized void dispose(ClientConnection clientConnection)
    {
        clientConnection.dispose();
        if (clientConnection.Users == 0)
            ClientConnections.remove(clientConnection.Key);
    }
}
