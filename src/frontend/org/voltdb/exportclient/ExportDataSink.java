/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.exportclient;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Map.Entry;

import org.voltdb.elt.ELTProtoMessage;


public class ExportDataSink implements Runnable
{
    private int m_tableId = -1;
    private int m_partitionId = -1;
    private String m_tableName;
    private ExportDecoderBase m_decoder;
    private String m_activeConnection = null;

    private HashMap<String, LinkedList<ELTProtoMessage>> m_rxQueues;
    private HashMap<String, LinkedList<ELTProtoMessage>> m_txQueues;

    boolean m_started = false;

    public ExportDataSink(int partitionId, int tableId,
                      String tableName, ExportDecoderBase decoder)
    {
        m_tableId = tableId;
        m_partitionId = partitionId;
        m_tableName = tableName;
        m_decoder = decoder;
        m_rxQueues = new HashMap<String, LinkedList<ELTProtoMessage>>();
        m_txQueues = new HashMap<String, LinkedList<ELTProtoMessage>>();
    }

    public int getTableId()
    {
        return m_tableId;
    }

    public int getPartitionId()
    {
        return m_partitionId;
    }

    void addELConnection(String connectionName)
    {
        if (m_activeConnection == null)
        {
            m_activeConnection = connectionName;
        }
        m_rxQueues.put(connectionName, new LinkedList<ELTProtoMessage>());
        m_txQueues.put(connectionName, new LinkedList<ELTProtoMessage>());
    }

    Queue<ELTProtoMessage> getRxQueue(String connectionName)
    {
        return m_rxQueues.get(connectionName);
    }

    Queue<ELTProtoMessage> getTxQueue(String connectionName)
    {
        return m_txQueues.get(connectionName);
    }

    // XXX hacky "for when we move to threading" blah
    @Override
    public void run()
    {
        while (true)
        {
            work();
        }
    }

    public void work()
    {
        if (!m_started)
        {
            poll();
            m_started = true;
        }
        for (Entry<String, LinkedList<ELTProtoMessage>> rx_conn : m_rxQueues.entrySet())
        {
            ELTProtoMessage m = rx_conn.getValue().poll();
            if (m != null && m.isPollResponse())
            {
                m_activeConnection = rx_conn.getKey();
                handlePollResponse(m);
            }
        }
    }

    private void poll()
    {
        System.out.println("Polling table " + m_tableName +
                           ", partition " + m_partitionId + " for new data.");

        ELTProtoMessage m = new ELTProtoMessage(m_partitionId, m_tableId);
        m.poll();
        m_txQueues.get(m_activeConnection).offer(m);
    }

    private void pollAndAck(ELTProtoMessage prev)
    {
        System.out.println("Poller, table " + m_tableName + ": pollAndAck " + prev.getAckOffset());
        ELTProtoMessage next = new ELTProtoMessage(m_partitionId, m_tableId);
        next.poll().ack(prev.getAckOffset());
        ELTProtoMessage ack = new ELTProtoMessage(m_partitionId, m_tableId);
        ack.ack(prev.getAckOffset());
        for (String connectionName : m_txQueues.keySet())
        {
            if (connectionName.equals(m_activeConnection))
            {
//                System.out.println("POLLANDACK: " + connectionName + ", offset: " + prev.getAckOffset());
                m_txQueues.get(m_activeConnection).offer(next);
            }
            else
            {
//                System.out.println("ACK: " + connectionName + ", offset: " + prev.getAckOffset());
                m_txQueues.get(connectionName).offer(ack);
            }
        }
    }

    private void handlePollResponse(ELTProtoMessage m)
    {
        // if a poll returns no data, this process is complete.
        if (m.getData().remaining() == 0) {
            m_decoder.noDataReceived(m.getAckOffset());
            poll();
            return;
        }

        // read the streamblock length prefix.
        int ttllength = m.getData().getInt();
        System.out.println("Poller: table: " + m_tableName +
                           ", partition: " + m_partitionId +
                           " : data payload bytes: " + ttllength);

        // a stream block prefix of 0 also means empty queue.
        if (ttllength == 0) {
            m_decoder.noDataReceived(m.getAckOffset());
            poll();
            return;
        }

        // run the verifier until m.getData() is consumed
        while (m.getData().hasRemaining()) {
            int length = m.getData().getInt();
            byte[] rowdata = new byte[length];
            m.getData().get(rowdata, 0, length);
            m_decoder.processRow(length, rowdata);
        }

        // ack the old block and poll the next.
        pollAndAck(m);
    }
}
