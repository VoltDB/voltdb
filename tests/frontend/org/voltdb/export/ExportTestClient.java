/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.export;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportConnection;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.logging.VoltLogger;

public class ExportTestClient extends ExportClientBase
{
    private static final VoltLogger m_logger = new VoltLogger("ExportClient");
    // hash table name + partition to verifier
    private final HashMap<String, ExportTestVerifier> m_verifiers =
        new HashMap<String, ExportTestVerifier>();

    private HashMap<String, ExportTestVerifier> m_verifiersToReserve = new HashMap<String, ExportTestVerifier>();

    public ExportTestClient(int nodeCount)
    {
        super.addServerInfo(new InetSocketAddress("localhost", VoltDB.DEFAULT_PORT));
    }

    /*
     * The export client base is going to reconnect. Reserve it the old verifiers
     * that have the test data.
     */
    public void reserveVerifiers() {
        m_verifiersToReserve = new HashMap<String, ExportTestVerifier>(m_verifiers);
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source)
    {
        String key = source.tableName + source.partitionId;
        if (m_verifiersToReserve.containsKey(key)) {
            return m_verifiersToReserve.remove(key);
        }

        // create a verifier with the 'schema'
        ExportTestVerifier verifier = new ExportTestVerifier(source);
        // hash it by table name + partition ID
        m_logger.info("Creating verifier for table: " + source.tableName +
                           ", part ID: " + source.partitionId);
        if (!m_verifiers.containsKey(key))
        {
            m_verifiers.put(key,
                            verifier);
        }
        return verifier;
    }

    public void addRow(String tableName, Object partitionHash, Object[] data)
    {
        int partition = TheHashinator.hashToPartition(partitionHash);
        ExportTestVerifier verifier = m_verifiers.get(tableName + partition);
        if (verifier == null)
        {
            // something horribly wrong, bail
            System.out.println("No verifier for table " + tableName + " and partition " + partition);
            System.exit(1);
        }
        verifier.addRow(data);
    }

    private boolean done()
    {
        boolean retval = true;
        for (ExportTestVerifier verifier : m_verifiers.values())
        {
            if (!verifier.done())
            {
                retval = false;
            }
        }
        for (ExportConnection connection : m_exportConnections.values())
        {
            if (!connection.isConnected())
            {
                retval = true;
            }
        }
        return retval;
    }

    public boolean allRowsVerified()
    {
        boolean retval = true;
        for (ExportTestVerifier verifier : m_verifiers.values())
        {
            if (!verifier.allRowsVerified())
            {
                retval = false;
            }
        }
        return retval;
    }

    public boolean verifyExportOffsets()
    {
        boolean retval = true;

        HashMap<String, HashMap<Integer, Long>> table_offsets = new HashMap<String, HashMap<Integer,Long>>();

        // Generate polls for every connection/table/partition
        for (ExportConnection connection : m_exportConnections.values())
        {
            HashMap<String, HashMap<Integer, Long>> seen_responses = new HashMap<String, HashMap<Integer, Long>>();
            for (AdvertisedDataSource source : connection.dataSources)
            {
                try
                {
                    ExportProtoMessage poll = new ExportProtoMessage(source.partitionId,
                                                               source.signature);
                    poll.poll();
                    connection.sendMessage(poll);

                    // Poll this source on this connection
                    ExportProtoMessage m = null;
                    // We know all possibly outstanding responses will be fully
                    // drained, so just wait until we get any response for
                    // this data source
                    while (m == null || !m.getSignature().equals(source.signature)  ||
                           m.getPartitionId() != source.partitionId)
                    {
                        m = connection.nextMessage();
                    }
                    assert(m.isPollResponse());
                    long offset = m.getAckOffset();

                    // Now, see if we've seen this offset for this table.  If so,
                    // check to see that it's equal.  Otherwise, stash it.
                    HashMap<Integer, Long> offsets = table_offsets.get(m.m_signature);
                    if (offsets == null) {
                        offsets = new HashMap<Integer, Long>();
                        table_offsets.put(m.m_signature, offsets);
                    }

                    if (!offsets.containsKey(source.partitionId))
                    {
                        offsets.put( source.partitionId, offset);
                    }
                    else
                    {
                        if (offsets.get(source.partitionId) != offset)
                        {
                            System.out.println("Mismatched Export offset: " + offset);
                            System.out.println("  Table ID: " + source.tableName);
                            System.out.println("  Partition: " + source.partitionId);
                            System.out.println("  Orig. offset: " + offsets.get(m.m_partitionId));
                            retval = false;
                        }
                    }

                    HashMap<Integer, Long> responses = seen_responses.get(m.m_signature);
                    if (responses == null) {
                        responses = new HashMap<Integer, Long>();
                        seen_responses.put(m.m_signature, responses);
                    }
                    if (responses.containsKey(source.partitionId))
                    {
                        System.out.println("Saw duplicate response from connection: " +
                                           connection.name);
                        System.out.println("   for table: " + source.tableName +
                                           ", " + source.partitionId);
                        retval = false;
                    }
                    else
                    {
                        responses.put(source.partitionId, offset);
                    }
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                    System.exit(1);
                }
            }

            int seenResponseCount = 0;
            for (HashMap<Integer, Long> responses : seen_responses.values()) {
                seenResponseCount += responses.size();
            }
            org.voltdb.VoltType type = VoltType.BIGINT;
            switch (type) {
            case BIGINT:
                break;
            }
            if (seenResponseCount != connection.dataSources.size())
            {
                System.out.println("Didn't see enough responses from connection: " +
                                   connection.name + " saw "
                                   + seen_responses.entrySet().size() + " but expected "
                                   + connection.dataSources.size());
                retval = false;
            }
        }
        return retval;
    }

    @Override
    public int work() throws IOException
    {
        while (!done())
        {
            super.work();
        }
        return 1;
    }
}
