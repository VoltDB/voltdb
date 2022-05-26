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
/*
 * This client provideds a wrapper layer for running the Yahoo Cloud Serving
 * Benchmark (YCSB) against VoltDB. This benchmark runs a synchronous client
 * with a mix of the operations provided below. YCSB is open-source, and may
 * be found at https://github.com/brianfrankcooper/YCSB. The YCSB jar must be
 * in your classpath to compile this client.
 */
package com.yahoo.ycsb.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.CyclicBarrier;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

public class VoltClient4 extends DB {
    private Client m_client;
    private String[] m_partitionkeys;
    private byte[] m_workingData;
    private ByteBuffer m_writeBuf;

    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Override
    public void init() throws DBException
    {
        Properties props = getProperties();
        String servers = props.getProperty("voltdb.servers", "localhost");
        String user = props.getProperty("voltdb.user", "");
        String password = props.getProperty("voltdb.password", "");
        String strLimit = props.getProperty("voltdb.ratelimit");
        int ratelimit = strLimit != null ? Integer.parseInt(strLimit) : Integer.MAX_VALUE;
        try
        {
            m_client = ConnectionHelper.createConnection(Thread.currentThread().getId(), servers, user, password, ratelimit);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new DBException(e.getMessage());
        }
        m_workingData = new byte[1024 * 1024];
        m_writeBuf = ByteBuffer.wrap(m_workingData);
    }

    @Override
    public void cleanup() throws DBException
    {
        ConnectionHelper.disconnect(Thread.currentThread().getId());
    }

    @Override
    public int delete(String keyspace, String key)
    {
        try
        {
            ClientResponse response = m_client.callProcedure("STORE.delete", keyspace.getBytes(UTF8), key);
            return response.getStatus() == ClientResponse.SUCCESS ? 0 : 1;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return 1;
        }
    }

    @Override
    public int insert(String keyspace, String key, HashMap<String, ByteIterator> columns)
    {
        return update(keyspace, key, columns);
    }

    @Override
    public int read(String keyspace, String key, Set<String> columns, HashMap<String, ByteIterator> result)
    {
        try
        {
            ClientResponse response = m_client.callProcedure("Get", keyspace.getBytes(UTF8), key);
            if (response.getStatus() != ClientResponse.SUCCESS)
            {
                return 1;
            }
            VoltTable table = response.getResults()[0];
            if (table.advanceRow())
            {
                unpackRowData(table, columns, result);
            }
            return 0;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return 1;
        }
    }

    @Override
    public int scan(String keyspace, String lowerBound, int recordCount, Set<String> columns, Vector<HashMap<String, ByteIterator>> result)
    {
        try
        {
            byte[] ks = keyspace.getBytes(UTF8);
            ClientResponse response = m_client.callProcedure("Scan", ks, lowerBound, lowerBound.getBytes(UTF8), recordCount);
            if (response.getStatus() != ClientResponse.SUCCESS)
            {
                return 1;
            }

            int nFound = 0;
            String partKey = lowerBound;
            CyclicBarrier barrier = new CyclicBarrier(2);
            result.ensureCapacity(recordCount);
            ScanCallback callback = null;
            boolean proceed = true;
            while (proceed)
            {
                if (response.getStatus() != ClientResponse.SUCCESS)
                {
                    return 1;
                }
                VoltTable table = response.getResults()[0];
                nFound += table.getRowCount();
                proceed = nFound < recordCount && (partKey = nextPartitionKey(partKey)) != null;
                if (proceed)
                {
                    barrier.reset();
                    callback = new ScanCallback(barrier);
                    m_client.callProcedure(callback, "Scan", ks, partKey, null, recordCount - nFound);
                }

                while (table.advanceRow())
                {
                    result.add(unpackRowData(table, columns));
                }

                if (proceed)
                {
                    barrier.await();
                    response = callback.response;
                }
            }
            return 0;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return 1;
        }
    }

    @Override
    public int update(String keyspace, String key, HashMap<String, ByteIterator> columns)
    {
        try
        {
            ClientResponse response = m_client.callProcedure("Put", keyspace.getBytes(UTF8), key, packRowData(columns));
            return response.getStatus() == ClientResponse.SUCCESS ? 0 : 1;
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return 1;
        }
    }

    private String nextPartitionKey(String key) throws NoConnectionsException, IOException, ProcCallException
    {
        if (m_partitionkeys == null)
        {
            initializePartitionKeys();
        }
        long nextPartition = ((ClientImpl) m_client).getPartitionForParameter(VoltType.STRING.getValue(), key) + 1;
        if (nextPartition >= m_partitionkeys.length)
        {
           return null;
        }
        return m_partitionkeys[(int) nextPartition];
    }

    private void initializePartitionKeys() throws NoConnectionsException, IOException, ProcCallException
    {
        VoltTable keyTables = m_client.callProcedure("@GetPartitionKeys", "STRING").getResults()[0];
        m_partitionkeys = new String[keyTables.getRowCount()];
        while (keyTables.advanceRow())
        {
            String partkey = keyTables.getString(1);
            int partition = (int) ((ClientImpl) m_client).getPartitionForParameter(VoltType.STRING.getValue(), partkey);
            m_partitionkeys[partition] = partkey;
        }
    }

    private static class ScanCallback implements ProcedureCallback
    {
        CyclicBarrier barrier;
        ClientResponse response;

        ScanCallback(CyclicBarrier barrier)
        {
            this.barrier = barrier;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception
        {
            response = clientResponse;
            barrier.await();
        }
    }

    private byte[] packRowData(HashMap<String, ByteIterator> columns)
    {
        m_writeBuf.clear();
        m_writeBuf.putInt(columns.size());
        for (String key : columns.keySet())
        {
            byte[] k = key.getBytes(UTF8);
            m_writeBuf.putInt(k.length);
            m_writeBuf.put(k);

            ByteIterator v = columns.get(key);
            int len = (int) v.bytesLeft();
            m_writeBuf.putInt(len);
            v.nextBuf(m_workingData, m_writeBuf.position());
            m_writeBuf.position(m_writeBuf.position() + len);
        }

        byte[] data = new byte[m_writeBuf.position()];
        System.arraycopy(m_workingData, 0, data, 0, data.length);
        return data;
    }

    private HashMap<String, ByteIterator> unpackRowData(VoltTable data, Set<String> fields)
    {
        byte[] rowData = data.getVarbinary(0);
        ByteBuffer buf = ByteBuffer.wrap(rowData);
        int nFields = buf.getInt();
        int size = fields != null ? Math.min(fields.size(), nFields) : nFields;
        HashMap<String, ByteIterator> res = new HashMap<String, ByteIterator>(size, (float) 1.25);
        return unpackRowData(rowData, buf, nFields, fields, res);
    }

    private HashMap<String, ByteIterator> unpackRowData(VoltTable data, Set<String> fields, HashMap<String, ByteIterator> result)
    {
        byte[] rowData = data.getVarbinary(0);
        ByteBuffer buf = ByteBuffer.wrap(rowData);
        int nFields = buf.getInt();
        return unpackRowData(rowData, buf, nFields, fields, result);
    }

    private HashMap<String, ByteIterator> unpackRowData(byte[] rowData, ByteBuffer buf, int nFields, Set<String> fields, HashMap<String, ByteIterator> result)
    {
        for (int i = 0; i < nFields; i++)
        {
            int len = buf.getInt();
            int off = buf.position();
            String key = new String(rowData, off, len, UTF8);
            buf.position(off + len);
            len = buf.getInt();
            off = buf.position();
            if (fields == null || fields.contains(key))
            {
                result.put(key, new ByteArrayByteIterator(rowData, off, len));
            }
            buf.position(off + len);
        }
        return result;
    }
}
