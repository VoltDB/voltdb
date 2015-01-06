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

package com;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import com.eng866.Insert;

public class Eng866Client
{
    final static int NUM_NAMES = 50;
    static int m_averageBatchSize = 25000;
    static int m_batchesToKeep = 12;
    static int m_deceasedCleanupFreq = -1;
    static int m_snapshotFreq = -1;
    static int m_maxBatchFreq = 10;
    static boolean m_blockingSnapshots = false;
    static boolean m_smallStrings = false;
    static String m_snapshotId = "Deletes";
    static String m_snapshotDir = "/tmp/deletes";
    static String[] m_names = new String[NUM_NAMES];
    static Random m_rand = new Random(System.currentTimeMillis());
    static long m_batchNumber = 1000;
    static int m_totalRows;
    static long m_highAllocMem = 0;
    static long m_highAllocMemTime = 0;
    static long m_highUsedMem = 0;
    static long m_highUsedMemTime = 0;
    static long m_highRss = 0;
    static long m_highRssTime = 0;
    static long m_totalInserts = 0;
    static long m_totalInsertedRows = 0;
    static long m_totalInsertTime = 0;
    static long m_expectedInserts = 0;
    static long m_totalDeletes = 0;
    static long m_totalDeletedRows = 0;
    static long m_totalDeleteTime = 0;
    static long m_expectedDeletes = 0;
    static long m_totalDeadDeletes = 0;
    static long m_totalDeadDeleteTime = 0;
    static long m_expectedDeadDeletes = 0;
    static long m_expectedCounts = 0;
    static ArrayList<Integer> m_snapshotSizes = new ArrayList<Integer>();
    static boolean m_snapshotInProgress = false;

    static long m_timestamp = 0;

    static String randomString(int maxStringSize)
    {
        final String lazyletters = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder();
        int stringSize = m_rand.nextInt(maxStringSize) + 1;
        for (int j = 0; j < stringSize; j++)
        {
            int index = m_rand.nextInt(lazyletters.length());
            sb.append(lazyletters.charAt(index));
        }
        return sb.toString();
    }

    static void insertNewHashtag(Client client, String hashtag, long time)
    {
        try
        {
            while (!client.callProcedure(new ProcedureCallback()
                {
                    @Override
                    public void clientCallback(ClientResponse response) {
                        if (response.getStatus() != ClientResponse.SUCCESS){
                            System.out.println("failed insert");
                            System.out.println(response.getStatusString());
                        }
                        m_expectedCounts--;
                    }
                },
                Insert.class.getSimpleName(), hashtag, time)
                )
            {
                try
                {
                    client.backpressureBarrier();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        } catch (NoConnectionsException e) {
            System.err.println("Lost connection to database, terminating");
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void insertNewTweet(Client client, String username, long time)
    {
        try
        {
            while (!client.callProcedure(new ProcedureCallback()
                {
                    @Override
                    public void clientCallback(ClientResponse response) {
                        if (response.getStatus() != ClientResponse.SUCCESS){
                            System.out.println("failed insert");
                            System.out.println(response.getStatusString());
                        }
                        m_expectedCounts--;
                    }
                },
                "InsertTweet", username, time)
                )
            {
                try
                {
                    client.backpressureBarrier();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        } catch (NoConnectionsException e) {
            System.err.println("Lost connection to database, terminating");
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void parseStats(ClientResponse resp)
    {
        // Go ghetto for now and assume we're running on one host.
        VoltTable memory_stats = resp.getResults()[0];
        //System.out.println("mem stats: " + memory_stats);
        long rss = memory_stats.fetchRow(0).getLong("RSS");
        if (rss > m_highRss)
        {
            m_highRss = rss;
            m_highRssTime = System.currentTimeMillis();
        }
        long alloc_mem = 0;
        long used_mem = 0;
        alloc_mem += memory_stats.fetchRow(0).getLong("JAVAUSED");
        alloc_mem += memory_stats.fetchRow(0).getLong("TUPLEALLOCATED");
        alloc_mem += memory_stats.fetchRow(0).getLong("INDEXMEMORY");
        alloc_mem += memory_stats.fetchRow(0).getLong("POOLEDMEMORY");
        used_mem += memory_stats.fetchRow(0).getLong("JAVAUSED");
        used_mem += memory_stats.fetchRow(0).getLong("TUPLEDATA");
        used_mem += memory_stats.fetchRow(0).getLong("INDEXMEMORY");
        used_mem += memory_stats.fetchRow(0).getLong("STRINGMEMORY");
        if (alloc_mem > m_highAllocMem)
        {
            m_highAllocMem = alloc_mem;
            m_highAllocMemTime = System.currentTimeMillis();
        }
        if (used_mem > m_highUsedMem)
        {
            m_highUsedMem = used_mem;
            m_highUsedMemTime = System.currentTimeMillis();
        }
        System.out.println("CURRENT MEMORY TOTALS (USED, ALLOCATED, RSS):");
        System.out.println("CURRENT," + used_mem * 1000 + "," + alloc_mem * 1000 + "," + rss * 1000);
        Date blah = new Date(m_highUsedMemTime);
        System.out.println("LARGEST MEMORY USED: " + m_highUsedMem * 1000 + " at " + blah.toString());
        blah = new Date(m_highAllocMemTime);
        System.out.println("LARGEST MEMORY ALLOCATED: " + m_highAllocMem * 1000 + " at " + blah.toString());
        blah = new Date(m_highRssTime);
        System.out.println("LARGEST RSS: " + m_highRss * 1000 + " at " + blah.toString());
    }

    static void collectStats(Client client)
    {
        try {
            ClientResponse resp = client.callProcedure("@Statistics", "management", 0);
            parseStats(resp);
        } catch (NoConnectionsException e) {
            System.err.println("Lost connection to database, terminating");
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
        }
        catch (ProcCallException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    static void getHashTags(Client client, long time, int count)
    {
        //System.out.println("Getting " + count + " tags newer than time: " + time);
        try
        {
            while (!client.callProcedure(new ProcedureCallback()
            {
                @Override
                public void clientCallback(ClientResponse response) {
                    if (response.getStatus() != ClientResponse.SUCCESS){
                        System.out.println("failed count batch");
                        System.out.println(response.getStatusString());
                    }
                    else
                    {
                        m_expectedCounts--;
                    }
                }
            },
            "Select", time, count)
            )
            {
                try
                {
                    client.backpressureBarrier();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        catch (NoConnectionsException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args)
    {
        int hash_preload = Integer.valueOf(args[0]);
        String commaSeparatedServers = args[1];

        // parse the server list
        List<String> servers = new LinkedList<String>();
        String[] commaSeparatedServersParts = commaSeparatedServers.split(",");
        for (String server : commaSeparatedServersParts) {
            servers.add(server.trim());
        }

        Client client = null;
        ClientConfig config = new ClientConfig("program", "none");
        client = ClientFactory.createClient(config);
        for (String server : servers) {
            try {
                client.createConnection(server);
            } catch (UnknownHostException e) {
                e.printStackTrace();
                System.exit(-1);
            } catch (IOException e) {
                System.err.println("Could not connect to database, terminating: (" + server + ")");
                System.exit(-1);
            }
        }

        // Fill a bunch of data into the hashtag table so the query will spin
        System.out.println("Inserting " + hash_preload + " hashtag entries");
        for (int i = 0; i < hash_preload; i++)
        {
            insertNewHashtag(client, randomString(250), m_timestamp++);
        }

        // loop:
        // kick off the multi-part select
        // insert random data into the export table
        // pause briefly
        while (true)
        {
            System.out.println("Starting cycle: " + m_timestamp);
            m_expectedCounts = 2;
            String rando = randomString(250);
            getHashTags(client, 0, 10);
            insertNewTweet(client, rando, m_timestamp);
            try
            {
                Thread.sleep(500);
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            while (m_expectedCounts > 0)
            {
                Thread.yield();
            }
            System.out.println("Completed cycle");
            m_timestamp++;
        }
    }
}
