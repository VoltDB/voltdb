/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
package org.voltdb.elt;

import java.util.ArrayList;
import java.util.HashMap;

import org.voltdb.TheHashinator;
import org.voltdb.elclient.ELClientBase;
import org.voltdb.elclient.ELConnection;
import org.voltdb.elclient.ELDataSink;
import org.voltdb.elt.ELTProtoMessage.AdvertisedDataSource;

public class ELTestClient extends ELClientBase
{
    private ArrayList<AdvertisedDataSource> m_dataSources = null;
    private HashMap<String, ELTVerifier> m_verifiers =
        new HashMap<String, ELTVerifier>();

    @Override
    public void constructELTDataSinks(ELConnection elConnection)
    {
        m_dataSources = elConnection.getDataSources();
        for (AdvertisedDataSource source : m_dataSources)
        {
            // create a verifier with the 'schema'
            ELTVerifier verifier = new ELTVerifier(source.tableName(),
                                                   source.partitionId(),
                                                   source.columnTypes());
            // hash it by table name + partition ID
            System.out.println("Creating verifier for table: " + source.tableName() +
                               ", part ID: " + source.partitionId());
            m_verifiers.put(source.tableName() + source.partitionId(),
                            verifier);
            // create a protocol handler for the lot and register it with
            // the connection
            ELDataSink handler =
                new ELDataSink(source.partitionId(), source.tableId(),
                                      source.tableName(), verifier);
            elConnection.registerDataSink(handler);
        }
    }

    public void addRow(String tableName, Object partitionHash, Object[] data)
    {
        int partition = TheHashinator.hashToPartition(partitionHash);
        ELTVerifier verifier = m_verifiers.get(tableName + partition);
        if (verifier == null)
        {
            // something horribly wrong, bail
            System.exit(1);
        }
        verifier.addRow(data);
    }

    private boolean done()
    {
        boolean retval = true;
        for (ELTVerifier verifier : m_verifiers.values())
        {
            if (!verifier.done())
            {
                retval = false;
            }
        }
        return retval;
    }

    public boolean allRowsVerified()
    {
        boolean retval = true;
        for (ELTVerifier verifier : m_verifiers.values())
        {
            if (!verifier.allRowsVerified())
            {
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
