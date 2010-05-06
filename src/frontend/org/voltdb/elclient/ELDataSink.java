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

package org.voltdb.elclient;

import java.util.LinkedList;
import java.util.Queue;

import org.voltdb.elt.ELTProtoMessage;


public class ELDataSink implements Runnable
{
    private int m_tableId = -1;
    private int m_partitionId = -1;
    private String m_tableName;
    private ELTDecoderBase m_decoder;

    private LinkedList<ELTProtoMessage> m_rxQueue;
    private LinkedList<ELTProtoMessage> m_txQueue;

    boolean m_started = false;

    public ELDataSink(int partitionId, int tableId,
                      String tableName, ELTDecoderBase decoder)
    {
        m_tableId = tableId;
        m_partitionId = partitionId;
        m_tableName = tableName;
        m_decoder = decoder;
        m_rxQueue = new LinkedList<ELTProtoMessage>();
        m_txQueue = new LinkedList<ELTProtoMessage>();
    }

    public int getTableId()
    {
        return m_tableId;
    }

    public int getPartitionId()
    {
        return m_partitionId;
    }

    public Queue<ELTProtoMessage> getRxQueue()
    {
        return m_rxQueue;
    }

    public Queue<ELTProtoMessage> getTxQueue()
    {
        return m_txQueue;
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
        ELTProtoMessage m = m_rxQueue.poll();
        if (m != null && m.isPollResponse())
        {
            handlePollResponse(m);
        }
    }

    private void poll()
    {
        System.out.println("Polling table " + m_tableName +
                           ", partition " + m_partitionId + " for new data.");

        ELTProtoMessage m = new ELTProtoMessage(m_partitionId, m_tableId);
        m.poll();
        m_txQueue.offer(m);
    }

    private void pollAndAck(ELTProtoMessage prev)
    {
        System.out.println("Poller, table " + m_tableName + ": pollAndAck " + prev.getAckOffset());
        ELTProtoMessage next = new ELTProtoMessage(m_partitionId, m_tableId);
        next.poll().ack(prev.getAckOffset());
        m_txQueue.offer(next);
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
