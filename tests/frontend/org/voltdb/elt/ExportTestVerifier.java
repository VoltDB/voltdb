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

import java.io.IOException;
import java.util.ArrayDeque;

import org.voltdb.elt.ELTProtoMessage.AdvertisedDataSource;
import org.voltdb.exportclient.ExportDecoderBase;

class ExportTestVerifier extends ExportDecoderBase
{
    private final ArrayDeque<Object[]> m_data;
    private boolean m_rowFailed = false;
    private long m_lastAck = -1;
    private int m_ackRepeats = 0;
    private final String m_tableName;
    private final int m_partitionId;

    ExportTestVerifier(AdvertisedDataSource source)
    {
        super(source);
        m_tableName = source.tableName();
        m_partitionId = source.partitionId();
        m_data = new ArrayDeque<Object[]>();
    }

    void addRow(Object[] data)
    {
        m_data.offer(data);
    }

    @Override
    public void noDataReceived(long ackOffset)
    {
        if (ackOffset != m_lastAck)
        {
            m_lastAck = ackOffset;
            m_ackRepeats = 0;
        }
        else
        {
            m_ackRepeats++;
        }
    }

    @Override
    public boolean processRow(int rowSize, byte[] rowData)
    {
        boolean result = false;
        final Object[] srcdata = m_data.poll();

        // no data found - an ERROR.
        if (srcdata == null) {
            System.out.println("No source data. Rows remaining: " + m_data.size() +
                               " received: " + rowData.length + " bytes to verify.");
            m_rowFailed = true;
            return false;
        }

        // otherwise, if an error was already registered, preserve that state
        if (m_rowFailed) {
            return false;
        }

        try {
            Object[] decoded = decodeRow(rowData);
            // iterate the schema, verify the row data
            // skip 6 cols into the ELT schema since it includes the ELT columns
            // but then we need to back up the index into srcdata.
            for (int i = 6; i < m_tableSchema.size(); i++)
            {
                if (!(decoded[i].equals(srcdata[i-6])))
                {
                    System.out.println("Failed on table column: " + (i-6));
                    System.out.println("  orig value:" + srcdata[i-6].toString());
                    System.out.println("  elt value:" + decoded[i].toString());
                    m_rowFailed = true;
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
            m_rowFailed = true;
            return false;
        }
        return result;
    }

    boolean done()
    {
        return (m_ackRepeats > 3); // arbitrary value
    }

    boolean empty()
    {
        return (m_data.size() == 0);
    }

    boolean allRowsVerified()
    {
        boolean result = (m_data.size() == 0) && (!m_rowFailed);
        if (!result) {
            System.out.println("ELTVerifier error. Table ID: " + m_tableName +
                               ", partition ID: " + m_partitionId);
            System.out.println("  Data size: " +
                               m_data.size() + " row failed state: " + m_rowFailed);
        }
        return result;

    }
}
