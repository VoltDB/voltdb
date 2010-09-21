/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
import java.util.ArrayList;
import java.util.HashMap;

import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportConnection;
import org.voltdb.exportclient.ExportDecoderBase;

public class ExportTestClient extends ExportClientBase
{
    // hash table name + partition to verifier
    private final HashMap<String, ExportTestVerifier> m_verifiers =
        new HashMap<String, ExportTestVerifier>();

    public ExportTestClient(int nodeCount)
    {
        ArrayList<InetSocketAddress> servers =
            new ArrayList<InetSocketAddress>();
        for (int i = 0; i < nodeCount; i++)
        {
            servers.add(new InetSocketAddress("localhost", VoltDB.DEFAULT_PORT + i));
        }
        super.setServerInfo(servers);
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source)
    {
        // create a verifier with the 'schema'
        ExportTestVerifier verifier = new ExportTestVerifier(source);
        // hash it by table name + partition ID
        System.out.println("Creating verifier for table: " + source.tableName() +
                           ", part ID: " + source.partitionId());
        if (!m_verifiers.containsKey(source.tableName() + source.partitionId()))
        {
            m_verifiers.put(source.tableName() + source.partitionId(),
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

        HashMap<Long, Long> table_offsets = new HashMap<Long, Long>();

        // Generate polls for every connection/table/partition
        for (ExportConnection connection : m_exportConnections.values())
        {
            HashMap<Long, Long> seen_responses = new HashMap<Long, Long>();
            for (AdvertisedDataSource source : connection.getDataSources())
            {
                try
                {
                    ExportProtoMessage poll = new ExportProtoMessage(source.partitionId(),
                                                               source.tableId());
                    poll.poll();
                    connection.sendMessage(poll);

                    // Poll this source on this connection
                    ExportProtoMessage m = null;
                    // We know all possibly outstanding responses will be fully
                    // drained, so just wait until we get any response for
                    // this data source
                    while (m == null || m.getTableId() != source.tableId() ||
                           m.getPartitionId() != source.partitionId())
                    {
                        m = connection.nextMessage();
                    }
                    assert(m.isPollResponse());
                    long offset = m.getAckOffset();
                    // Now, see if we've seen this offset for this table.  If so,
                    // check to see that it's equal.  Otherwise, stash it.
                    Long table_hash = m.getTableId() * 137 + m.getPartitionId();
                    if (!table_offsets.containsKey(table_hash))
                    {
                        table_offsets.put(table_hash, offset);
                    }
                    else
                    {
                        if (table_offsets.get(table_hash) != offset)
                        {
                            System.out.println("Mismatched Export offset: " + offset);
                            System.out.println("  Table ID: " + source.tableName());
                            System.out.println("  Partition: " + source.partitionId());
                            System.out.println("  Orig. offset: " + table_offsets.get(table_hash));
                            retval = false;
                        }
                    }
                    if (seen_responses.containsKey(table_hash))
                    {
                        System.out.println("Saw duplicate response from connection: " +
                                           connection.getConnectionName());
                        System.out.println("   for table: " + source.tableName() +
                                           ", " + source.partitionId());
                        retval = false;
                    }
                    else
                    {
                        seen_responses.put(table_hash, offset);
                    }
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
            if (seen_responses.entrySet().size() != connection.getDataSources().size())
            {
                System.out.println("Didn't see enough responses from connection: " +
                                   connection.getConnectionName());
                retval = false;
            }
        }
        return retval;
    }

    @Override
    public void work()
    {
        while (!done())
        {
            super.work();
        }
    }
}
