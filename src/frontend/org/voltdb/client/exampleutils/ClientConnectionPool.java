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
package org.voltdb.client.exampleutils;

import org.voltdb.client.Client;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ClientConnectionPool
{
    private static final Lock lock = new ReentrantLock();
    private static final ConcurrentHashMap<String,PerfCounterMap> Statistics = new ConcurrentHashMap<String,PerfCounterMap>();
    private static final short MAX_USERS_PER_CLIENT = 50;
    private static final ConcurrentHashMap<String,ClientConnection> ClientConnections = new ConcurrentHashMap<String,ClientConnection>();

    private static String getClientConnectionKeyBase(String[] servers, int port, String user, String password, boolean isHeavyWeight, int maxOutstandingTxns)
    {
        String clientConnectionKeyBase = user + ":" + password + "@";
        for(int i=0;i<servers.length;i++)
            clientConnectionKeyBase += servers[i].trim() + ",";
        clientConnectionKeyBase += ":" + Integer.toString(port) + "{" + Boolean.toString(isHeavyWeight) + ":" + Integer.toString(maxOutstandingTxns) + "}";
        return clientConnectionKeyBase;
    }

    public static ClientConnection get(String servers, int port) throws Exception
    {
        return get(servers.split(","), port, "", "", false, 0);
    }
    public static ClientConnection get(String[] servers, int port) throws Exception
    {
        return get(servers, port, "", "", false, 0);
    }
    public static ClientConnection get(String servers, int port, String user, String password, boolean isHeavyWeight, int maxOutstandingTxns) throws Exception
    {
        return get(servers.split(","), port, user, password, isHeavyWeight, maxOutstandingTxns);
    }
    public static ClientConnection get(String[] servers, int port, String user, String password, boolean isHeavyWeight, int maxOutstandingTxns) throws Exception
    {
        String clientConnectionKeyBase = getClientConnectionKeyBase(servers, port, user, password, isHeavyWeight, maxOutstandingTxns);
        String clientConnectionKey = clientConnectionKeyBase;
        int cnt = 1;
        lock.lock();
        try
        {
            while(ClientConnections.containsKey(clientConnectionKey))
            {
                if (ClientConnections.get(clientConnectionKey).Users >= MAX_USERS_PER_CLIENT)
                    clientConnectionKey += "::" + cnt++;
                else
                    break;
            }
            if (!ClientConnections.containsKey(clientConnectionKey))
                ClientConnections.put(clientConnectionKey, new ClientConnection(clientConnectionKeyBase, clientConnectionKey, servers, port, user, password, isHeavyWeight, maxOutstandingTxns));
        }
        finally
        {
            lock.unlock();
        }
        return ClientConnections.get(clientConnectionKey).use();
    }
    public static void dispose(ClientConnection clientConnection)
    {
        lock.lock();
        try
        {
            clientConnection.dispose();
            if (clientConnection.Users == 0)
                ClientConnections.remove(clientConnection.Key);
        }
        finally
        {
            lock.unlock();
        }
    }

    public static PerfCounterMap getStatistics(ClientConnection connection)
    {
        return getStatistics(connection.KeyBase);
    }

    public static PerfCounterMap getStatistics(String servers, int port)
    {
        return getStatistics(getClientConnectionKeyBase(servers.split(","), port, "", "", false, 0));
    }
    public static PerfCounterMap getStatistics(String[] servers, int port)
    {
        return getStatistics(getClientConnectionKeyBase(servers, port, "", "", false, 0));
    }
    public static PerfCounterMap getStatistics(String servers, int port, String user, String password, boolean isHeavyWeight, int maxOutstandingTxns)
    {
        return getStatistics(getClientConnectionKeyBase(servers.split(","), port, user, password, isHeavyWeight, maxOutstandingTxns));
    }
    public static PerfCounterMap getStatistics(String[] servers, int port, String user, String password, boolean isHeavyWeight, int maxOutstandingTxns)
    {
        return getStatistics(getClientConnectionKeyBase(servers, port, user, password, isHeavyWeight, maxOutstandingTxns));
    }

    protected static PerfCounterMap getStatistics(String clientConnectionKeyBase)
    {
        // Admited: could get a little race condition at the very beginning, but all that'll happen is that we'll lose a handful of tracking event, a loss far outweighed by overall reduced contention.
        if(!Statistics.containsKey(clientConnectionKeyBase))
            Statistics.put(clientConnectionKeyBase, new PerfCounterMap());
        return Statistics.get(clientConnectionKeyBase);
    }
}
