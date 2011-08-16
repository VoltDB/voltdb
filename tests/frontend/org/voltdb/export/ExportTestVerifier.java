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
import java.util.ArrayDeque;

import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.utils.Encoder;

class ExportTestVerifier extends ExportDecoderBase
{
    private final ArrayDeque<Object[]> m_data;
    private boolean m_rowFailed = false;
    private long m_lastAck = -1;
    private int m_ackRepeats = 0;
    private final String m_tableName;
    private final int m_partitionId;
    private long sequenceNumber = 0;

    ExportTestVerifier(AdvertisedDataSource source)
    {
        super(source);
        m_tableName = source.tableName;
        m_partitionId = source.partitionId;
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
            try {
                Object[] decoded = decodeRow(rowData);
                StringBuilder sb = new StringBuilder();
                boolean first = true;
                for (Object obj : decoded) {
                    if (!first) {
                        sb.append(", ");
                    } else {
                        first = false;
                    }
                    sb.append(obj);
                }
                System.out.println(sb);
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("No source data. Rows remaining: " + m_data.size() +
                               " received: " + rowData.length + " bytes to verify.");
            m_rowFailed = true;
            return false;
        }

        // otherwise, if an error was already registered, preserve that state
        if (m_rowFailed) {
//            Object[] decoded = null;
//            try {
//                decoded = decodeRow(rowData);
//            } catch (IOException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//            StringBuffer sb = new StringBuffer();
//            sb.append("Expected: ");
//            for (Object o : srcdata) {
//                sb.append(o).append(',');
//            }
//            sb.append("\nReceived: ");
//            for (int i = 5; i < m_tableSchema.size(); i++) {
//                sb.append(decoded[i]).append(',');
//            }
//            sb.append('\n');
//            System.out.println(sb);
            return false;
        }

        try {
            Object[] decoded = decodeRow(rowData);
//            StringBuffer sb = new StringBuffer();
//            sb.append("Expected: ");
//            for (Object o : srcdata) {
//                sb.append(o).append(',');
//            }
//            sb.append("\nReceived: ");
//            for (int i = 5; i < m_tableSchema.size(); i++) {
//                sb.append(decoded[i]).append(',');
//            }
//            sb.append('\n');
//            System.out.println(sb);
            // iterate the schema, verify the row data
            // skip 5 cols into the Export schema since it includes the Export columns
            // (we check the operation type), but then we need to back up the
            // index into srcdata.
            for (int i = 5; i < m_tableSchema.size(); i++)
            {
                Object toCompare1 = decoded[i];
                Object toCompare2 = srcdata[i-5];

                // convert binary to hex
                if (toCompare1 instanceof byte[]) {
                    toCompare1 = Encoder.hexEncode((byte[]) toCompare1);
                }
                if (toCompare2 instanceof byte[]) {
                    toCompare2 = Encoder.hexEncode((byte[]) toCompare2);
                }

                // check other data
                if (!(toCompare1.equals(toCompare2)))
                {
                    System.out.println("Failed on table column: " + (i-5));
                    System.out.println("  orig value:" + toCompare2.toString());
                    System.out.println("  export value:" + toCompare1.toString());
                    m_rowFailed = true;
                }
            }
            if (!decoded[2].equals(sequenceNumber)) {
                System.out.println("Failed on sequence number expected:" + sequenceNumber + "  exported:" + decoded[2]);
                m_rowFailed = true;
            }
            sequenceNumber++;
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
            System.out.println("ExportVerifier error. Table ID: " + m_tableName +
                               ", partition ID: " + m_partitionId);
            System.out.println("  Data size: " +
                               m_data.size() + " row failed state: " + m_rowFailed);
        }
        return result;

    }

    @Override
    public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
        m_ackRepeats = 100;
    }
}
