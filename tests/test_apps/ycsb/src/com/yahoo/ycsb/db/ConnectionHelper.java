/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/*
 * Manage a mapping of one VoltDB client to every 50 YCSB client threads.
 */
package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;

public class ConnectionHelper {
    private static final int THREADS_PER_CLIENT = 50;

    private static HashMap<Long, ClientConnection> clientMapping = new HashMap<Long, ClientConnection>();
    private static ClientConnection activeConnection = null;

    /**
     * Creates a factory used to connect to a VoltDB instance.
     * (Note that if a corresponding connection exists, all parameters other than 'servers' are ignored)
     * @param clientId A unique identifier for the connecting client
     * @param servers The comma separated list of VoltDB servers in hostname[:port] format that the instance will use.
     * @param user The username for the connection
     * @param password The password for the specified user
     * @param ratelimit A limit on the number of transactions per second for the VoltDB instance
     * @return The existing factory if a corresponding connection has already been created; the newly created
     * one otherwise.
     * @throws IOException Throws if a connection is already open with a different server string.
     * @throws InterruptedException
     */
    public synchronized static Client createConnection(Long clientId, String servers, String user, String password, int ratelimit) throws IOException, InterruptedException
    {
        ClientConnection conn = clientMapping.get(clientId);
        if (conn != null)
        {
            return conn.m_client;
        }
        if (activeConnection != null && activeConnection.m_connectionCount.get() <= THREADS_PER_CLIENT)
        {
            activeConnection.connect();
            clientMapping.put(clientId, activeConnection);
            return activeConnection.m_client;
        }
        ClientConfig config = new ClientConfig(user, password);
        config.setMaxTransactionsPerSecond(ratelimit);
        Client client = ClientFactory.createClient(config);
        connect(client, servers);
        activeConnection = new ClientConnection(client);
        clientMapping.put(clientId, activeConnection);
        return client;
    }

    public static void disconnect(Long clientId)
    {
        ClientConnection connection = clientMapping.get(clientId);
        if (connection != null)
        {
            connection.disconnect();
        }
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    private static void connectToOneServerWithRetry(final Client client, String server)
    {
        int sleep = 1000;
        while (true)
        {
            try
            {
                client.createConnection(server);
                break;
            }
            catch (Exception e)
            {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try
                {
                    Thread.sleep(sleep);
                }
                catch (Exception interruted) {}
                if (sleep < 8000)
                {
                    sleep += sleep;
                }
            }
        }
        System.out.printf("Connected to VoltDB node at: %s.\n", server);
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional).
     * @throws InterruptedException if anything bad happens with the threads.
     */
    private static void connect(final Client client, String servers) throws InterruptedException
    {
        System.out.println("Connecting to VoltDB...");

        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray)
        {
            new Thread(new Runnable() {
                @Override
                public void run()
                {
                    connectToOneServerWithRetry(client, server);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();
    }

    private static class ClientConnection
    {
        private Client m_client;
        private AtomicInteger m_connectionCount;

        ClientConnection(Client client)
        {
            m_client = client;
            m_connectionCount = new AtomicInteger(1);
        }

        void connect()
        {
            m_connectionCount.incrementAndGet();
        }

        void disconnect()
        {
            int count = m_connectionCount.decrementAndGet();
            if (count <= 0 && m_client != null)
            {
                synchronized (this)
                {
                    try
                    {
                        m_client.drain();
                        m_client.close();
                    }
                    catch (NoConnectionsException e)
                    {
                        e.printStackTrace();
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                    m_client = null;
                }
            }
        }
    }
}
