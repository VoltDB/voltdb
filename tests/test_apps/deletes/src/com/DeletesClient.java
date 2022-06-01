/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.exampleutils.AppHelper;
import org.voltdb.utils.SnapshotVerifier;
import org.voltcore.logging.VoltLogger;

import com.deletes.Insert;

public class DeletesClient
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

    static VoltLogger log = new VoltLogger("DeletesClient");

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

    static void generateNames(int nameSize)
    {
        for (int i = 0; i < NUM_NAMES; i++)
        {
            m_names[i] = randomString(nameSize);
        }
    }

    static void insertNewRecord(Client client, long batchNumber)
    {
        int name_idx = m_rand.nextInt(NUM_NAMES);
        long age = m_rand.nextInt(100);
        long weight = m_rand.nextInt(200);
        long ts = batchNumber;

        String desc1 = null;
        String desc2 = null;
        String addr1 = null;
        String addr2 = null;
        String addr3 = null;
        String text1 = null;
        String text2 = null;
        String sig = null;
        String company = null;
        String co_addr = null;

        if (m_smallStrings)
        {
            desc1 = randomString(60);
            desc2 = randomString(60);
            addr1 = randomString(30);
            addr2 = randomString(60);
            addr3 = randomString(60);
            text1 = randomString(60);
            text2 = randomString(32);
            sig = randomString(16);
            company = randomString(60);
            co_addr = randomString(60);
        }
        else
        {
            desc1 = randomString(250);
            desc2 = randomString(250);
            addr1 = randomString(30);
            addr2 = randomString(120);
            addr3 = randomString(60);
            text1 = randomString(120);
            text2 = randomString(32);
            sig = randomString(16);
            company = randomString(60);
            co_addr = randomString(250);
        }

        byte deceased = (byte) (m_rand.nextBoolean() ? 1 : 0);
        try
        {
            while (!client.callProcedure(new ProcedureCallback()
                {
                    @Override
                    public void clientCallback(ClientResponse response) {
                        if (response.getStatus() != ClientResponse.SUCCESS){
                            log.warn("failed insert");
                            log.warn(response.getStatusString());
                        }
                        m_expectedInserts--; //we don't care if tx fail, but don't get stuck in yield
                    }
                },
                Insert.class.getSimpleName(),
                m_names[name_idx],
                age,
                weight,
                desc1,
                desc2,
                addr1,
                addr2,
                addr3,
                text1,
                text2,
                sig,
                ts,
                company,
                co_addr,
                deceased)
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
            log.warn("Lost connection to database, terminating");
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void insertBatch(Client client, boolean max_batch)
    {
        Date date = new Date();
        log.info("Total rows currently: " + m_totalRows);
        int to_insert = m_rand.nextInt(m_averageBatchSize * 2) + 1;
        if (max_batch)
        {
            to_insert = m_averageBatchSize * 2;
        }
        log.info("\tInserting: " + to_insert + " rows");
        m_expectedInserts = to_insert;
        long start_time = System.currentTimeMillis();
        for (int j = 0; j < to_insert; j++)
        {
            insertNewRecord(client, m_batchNumber);
        }
        m_batchNumber++;
        m_totalRows += to_insert;
        while (m_expectedInserts != 0)
        {
            Thread.yield();
        }
        long elapsed = System.currentTimeMillis() - start_time;
        m_totalInserts += to_insert;
        m_totalInsertTime += elapsed;
        log.info("\tBatch: " + to_insert + " took " +
                           elapsed + " millis");
        log.info("\t\t (" + ((to_insert * 1000)/elapsed) + " tps)");
        log.info("\tTotal insert TPS: " + (m_totalInserts * 1000)/m_totalInsertTime);
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
        log.info("CURRENT MEMORY TOTALS (USED, ALLOCATED, RSS):");
        log.info("CURRENT," + used_mem * 1000 + "," + alloc_mem * 1000 + "," + rss * 1000);
        Date blah = new Date(m_highUsedMemTime);
        log.info("LARGEST MEMORY USED: " + m_highUsedMem * 1000 + " at " + blah.toString());
        blah = new Date(m_highAllocMemTime);
        log.info("LARGEST MEMORY ALLOCATED: " + m_highAllocMem * 1000 + " at " + blah.toString());
        blah = new Date(m_highRssTime);
        log.info("LARGEST RSS: " + m_highRss * 1000 + " at " + blah.toString());
    }

    static void collectStats(Client client)
    {
        try {
            ClientResponse resp = client.callProcedure("@Statistics", "management", 0);
            parseStats(resp);
        } catch (NoConnectionsException e) {
            log.error("Lost connection to database, terminating");
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

    static void deleteBatch(Client client, long batchesToKeep)
    {
        long prune_ts = m_batchNumber - batchesToKeep;
        Date date = new Date();
        log.info("Pruning batches older than batch: " + prune_ts);
        m_expectedDeletes = NUM_NAMES;
        long start_time = System.currentTimeMillis();
        for (int i = 0; i < NUM_NAMES; i++)
        {
            try
            {
                while (!client.callProcedure(new ProcedureCallback()
                {
                    @Override
                    public void clientCallback(ClientResponse response) {
                        if (response.getStatus() != ClientResponse.SUCCESS){
                            log.warn("failed delete batch");
                            System.out.println(response.getStatusString());
                        }
                        else
                        {
                            m_totalRows -= response.getResults()[0].asScalarLong();
                            m_totalDeletedRows += response.getResults()[0].asScalarLong();
                        }
                        m_expectedDeletes--;  //we don't care if tx fail, but don't get stuck in yield
                    }
                },
                "DeleteOldBatches", m_names[i], prune_ts)
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
        while (m_expectedDeletes != 0)
        {
            Thread.yield();
        }
        long elapsed = System.currentTimeMillis() - start_time;
        m_totalDeletes += NUM_NAMES;
        m_totalDeleteTime += elapsed;
        log.info("\tAfter delete, total rows: " + m_totalRows);
        log.info("\tDeleting batch: " + NUM_NAMES + " took " +
                           elapsed + " millis");
        log.info("\t\t (" + ((NUM_NAMES * 1000)/elapsed) + " tps)");
        log.info("\tTotal delete TPS: " + (m_totalDeletes * 1000)/m_totalDeleteTime);
        log.info("\tTotal delete RPS: " + (m_totalDeletedRows * 1000)/m_totalDeleteTime);
    }

    static void deleteDeceased(Client client)
    {
        Date date = new Date();
        log.info("Deleting deceased records...");
        m_expectedDeadDeletes = NUM_NAMES;
        long start_time = System.currentTimeMillis();
        for (int i = 0; i < NUM_NAMES; i++)
        {
            try
            {
                while (!client.callProcedure(new ProcedureCallback()
                {
                    @Override
                    public void clientCallback(ClientResponse response) {
                        if (response.getStatus() != ClientResponse.SUCCESS){
                            log.warn("failed delete deceased");
                            log.warn(response.getStatusString());
                        }
                        else
                        {
                            m_totalRows -= response.getResults()[0].asScalarLong();
                        }
                        m_expectedDeadDeletes--; //we don't care if tx fail, but don't get stuck in yield
                    }
                },
                "DeleteDeceased", m_names[i])
                )
                {
                    try {
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
        while (m_expectedDeadDeletes != 0)
        {
            Thread.yield();
        }
        long elapsed = System.currentTimeMillis() - start_time;
        m_totalDeadDeletes += NUM_NAMES;
        m_totalDeadDeleteTime += elapsed;
        log.info("\tAfter dead deletes, total rows: " + m_totalRows);
        log.info("\tDeleting deceased: " + NUM_NAMES + " took " +
                           elapsed + " millis");
        log.info("\t\t (" + ((NUM_NAMES * 1000)/elapsed) + " tps)");
        log.info("\tTotal delete TPS: " + (m_totalDeadDeletes * 1000)/m_totalDeadDeleteTime);
    }

    static void countBatch(Client client, long batch)
    {
        Date date = new Date();
        log.info("Counting batch: " + batch);
        m_expectedCounts = 1;
        long start_time = System.currentTimeMillis();
        for (int i = 0; i < 1; i++)
        {
            try
            {
                while (!client.callProcedure(new ProcedureCallback()
                {
                    @Override
                    public void clientCallback(ClientResponse response) {
                        if (response.getStatus() != ClientResponse.SUCCESS){
                            log.warn("failed count batch");
                            log.warn(response.getStatusString());
                        }
                        else
                        {
                            log.info("\tBatch has " +
                                               response.getResults()[0].asScalarLong() +
                                               " items");
                        }
                        m_expectedCounts--; //we don't care if tx fail, but don't get stuck in yield
                    }
                },
                "CountBatchSize", "", batch)
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
        while (m_expectedCounts != 0)
        {
            Thread.yield();
        }
        long elapsed = System.currentTimeMillis() - start_time;
    }

    // stolen from TestSaveRestoreSysproc
    static void validateSnapshot()
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream original = System.out;
        try {
            System.setOut(ps);
            String args[] = new String[] {
                    m_snapshotId,
                    "--dir",
                    m_snapshotDir
            };
            SnapshotVerifier.main(args);
            ps.flush();
            String reportString = baos.toString("UTF-8");
            if (reportString.startsWith("Snapshot corrupted"))
            {
                log.error(reportString);
                System.exit(-1);
            }
        } catch (UnsupportedEncodingException e) {}
          finally {
            System.setOut(original);
        }
        log.info("Snapshot verified");
    }

    public static void checkSnapshotComplete(Client client)
    {
        // Check for outstanding snapshot
        VoltTable[] results = null;
        try
        {
            results = client.callProcedure("@Statistics", "SnapshotStatus", 0).getResults();
        }
        catch (NoConnectionsException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        catch (IOException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        catch (ProcCallException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        m_snapshotInProgress = false;
        while (results[0].advanceRow())
        {
            Long end_time = results[0].getLong("END_TIME");
            if (end_time == 0)
            {
                m_snapshotInProgress = true;
                return;
            }
        }
        if (results[0].getRowCount() > 0)
        {
            validateSnapshot();
        }
    }

    public static void performSnapshot(Client client)
    {
        checkSnapshotComplete(client);
        if (m_snapshotInProgress)
        {
            log.info("Snapshot still in progress, bailing");
            return;
        }
        try
        {
            VoltTable[] results = client.callProcedure("@SnapshotDelete", new String[] {m_snapshotDir},
                                 new String[] {m_snapshotId}).getResults();
        }
        catch (NoConnectionsException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        catch (IOException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        catch (ProcCallException e1)
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        // m_totalRows should be accurate at this point
        m_snapshotSizes.add(m_totalRows);
        log.info("Performing Snapshot with total rows: " + m_totalRows);
        try
        {
            if (m_blockingSnapshots) {
                ClientResponse response = client.callProcedure("@SnapshotSave", m_snapshotDir, m_snapshotId, 1);
                if (response.getStatus() != ClientResponse.SUCCESS)
                {
                    log.error("failed snapshot");
                    log.error(response.getStatusString());
                }
            }
            else {
                client.callProcedure(
                                     new ProcedureCallback()
                                     {
                                         @Override
                                         public void clientCallback(ClientResponse response) {
                                             if (response.getStatus() != ClientResponse.SUCCESS)
                                             {
                                                 log.error("failed snapshot");
                                                 log.error(response.getStatusString());
                                             }
                                         }
                                     },
                                     "@SnapshotSave", m_snapshotDir, m_snapshotId, 0);
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
        catch (ProcCallException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args)
    {
        // Use the AppHelper utility class to retrieve command line application parameters
        // Define parameters and pull from command line
        AppHelper apph = new AppHelper(DeletesClient.class.getCanonicalName())
            .add("duration", "run_duration_in_seconds", "Benchmark duration, in seconds.", 240)
            .add("average-batch-size", "average_batch_size", "Average batch size", 150000)
            .add("batches", "num_batches_to_keep", "Number of batches to keep", 5)
            .add("cleanup-freq", "cleanup_frequency", "Cleanup frequency, in seconds.", 6)
            .add("snapshot-freq", "cycles_between_snapshots", "Snapshot frequency, in seconds. -1 to turn off snapshots", -1)
            .add("block-snapshots", "use_blocking_snapshots_snapshots", "Blocking snapshots (true|false)", "false")
            .add("small-strings", "use_inline_strings", "Forces all the strings to be inlined strings (true|false)", "false")
            .add("servers", "comma_separated_server_list", "List of VoltDB servers to connect to.", "localhost")

            .setArguments(args)
            ;

        m_averageBatchSize = apph.intValue("average-batch-size");
        m_batchesToKeep = apph.intValue("batches");
        m_deceasedCleanupFreq = apph.intValue("cleanup-freq");
        m_snapshotFreq = apph.intValue("snapshot-freq");
        m_blockingSnapshots = apph.booleanValue("block-snapshots");
        m_smallStrings = apph.booleanValue("small-strings");
        long duration = apph.longValue("duration");
        String commaSeparatedServers = apph.stringValue("servers");

        apph.validate("average-batch-size", (m_averageBatchSize > 0));
        apph.validate("batches", (m_batchesToKeep >= 0));
        apph.validate("duration", (duration >= 0));
        apph.validate("cleanup-freq", (m_deceasedCleanupFreq > 0));

        apph.printActualUsage();

        log.info("Starting Deletes app with:");
        log.info("Average batch size of "+ m_averageBatchSize);
        log.info("Keeping batches:"+ m_batchesToKeep);
        log.info("Cleaning up deceased every batches:"+ m_deceasedCleanupFreq);
        log.info("Snapshotting every batches:"+ m_snapshotFreq);

        // parse the server list
        List<String> servers = new LinkedList<String>();
        String[] commaSeparatedServersParts = commaSeparatedServers.split(",");
        for (String server : commaSeparatedServersParts) {
            servers.add(server.trim());
        }

        File tmpdir = new File(m_snapshotDir);
        tmpdir.mkdir();

        generateNames(16);
        Client client = null;
        ClientConfig config = new ClientConfig("program", "none");
        config.setProcedureCallTimeout(Long.MAX_VALUE);
        config.setTopologyChangeAware(true);
        client = ClientFactory.createClient(config);
        // with topo awareness, we only need to connect to one server and it
        // will figure out the rest
        boolean success = false;
        Exception lastException = null;
        for (String server : servers) {
            try {
                client.createConnection(server);
            } catch (UnknownHostException e) {
                lastException = e;
                log.warn("can't connect to server:"+ server+" :"+e.getMessage());
                continue;
            } catch (IOException e) {
                lastException = e;
                log.warn("can't connect to server:"+ server+" :"+e.getMessage());
                continue;
            }
            log.info("connected to server "+server);
            success = true;
            break;
        }
        if ( ! success ) {
            log.error("Could not connect to database servers " + servers + "");
            lastException.printStackTrace();
            System.exit(-1);
        }

        final long endTime = System.currentTimeMillis() + (1000l * duration);
        final long startTime = System.currentTimeMillis();

        // Start with the maximum data set we could possibly fill
        for (int i = 0; i < m_batchesToKeep; i++)
        {
            insertBatch(client, true);
            log.info("batch "+i+"/"+m_batchesToKeep+" inserted");
        }

        // now add a batch and remove a batch
        long deceased_counter = 0;
        long snapshot_counter = 0;
        long max_batch_counter = 0;
        boolean fill_max = false;
        long max_batch_remaining = 0;

        while (endTime > System.currentTimeMillis())
        {
            // if (max_batch_counter == m_maxBatchFreq)
            // {
            //     fill_max = true;
            //     max_batch_remaining = m_batchesToKeep;
            //     max_batch_counter = 0;
            // }
            // else if (fill_max)
            // {
            //     max_batch_remaining--;
            //     fill_max = true;
            //     if (max_batch_remaining == 0)
            //     {
            //         fill_max = false;
            //     }
            // }
            // else
            // {
            //     max_batch_counter++;
            //     fill_max = false;
            // }

            insertBatch(client, fill_max);

            collectStats(client);

            snapshot_counter++;
            if (snapshot_counter == m_snapshotFreq)
            {
                performSnapshot(client);
                snapshot_counter = 0;
            }

            deceased_counter++;
            if (deceased_counter == m_deceasedCleanupFreq)
            {
                deleteDeceased(client);
                deceased_counter = 0;
            }
            countBatch(client, m_batchNumber - m_batchesToKeep - 1);

            deleteBatch(client, m_batchesToKeep);
        }
        log.info("Total runtime seconds:" + (System.currentTimeMillis() - startTime) / 1000l);
    }
}
