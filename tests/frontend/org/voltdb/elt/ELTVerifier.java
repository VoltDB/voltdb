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
import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;

import org.voltdb.VoltType;
import org.voltdb.elclient.ELTDecoderBase;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.types.TimestampType;

class ELTVerifier extends ELTDecoderBase
{
    private ArrayDeque<Object[]> m_data;
    private boolean m_rowFailed = false;
    private long m_lastAck = -1;
    private int m_ackRepeats = 0;
    private String m_tableName;
    private int m_partitionId;

    ELTVerifier(String tableName, int partitionId,
                ArrayList<VoltType> tableSchema)
    {
        super(tableSchema);
        m_tableName = tableName;
        m_partitionId = partitionId;
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
        final FastDeserializer fds = new FastDeserializer(rowData);
        final Object[] srcdata = m_data.poll();

        // no data found - an ERROR.
        if (srcdata == null) {
            m_rowFailed = true;
            return false;
        }

        // otherwise, if an error was already registered, preserve that state
        if (m_rowFailed) {
            return false;
        }

        // eltxxx: verify elt headers.
        // skip the null byte array - which length depends on the total column count
        // see EE's TupleStreamWrapper::computeOffsets()
        final int nullArrayLength = ((m_tableSchema.size() + 7) & -8) >> 3;

        try {
            // skip the elt header (txnid, timestamp, seqno, pid, sid, 'I'/'D')
            fds.skipBytes(nullArrayLength);
            fds.skipBytes(41);

            // iterate the schema, decode and verify the rowdata
            // skip 6 cols into the ELT schema since it includes the ELT columns
            // but then we need to back up the index into srcdata.
            for (int i=6; i < m_tableSchema.size(); i++) {
                switch (m_tableSchema.get(i)) {
                default:
                    assert(false) : "Unknown type in verifyRow()";
                    result = false;
                    m_rowFailed = true;
                    return false;
                case TINYINT:
                    result = decodeAndCompareTinyInt(srcdata[i - 6], fds);
                    if (!result) {
                        m_rowFailed = true;
                        return false;
                    }
                    break;
                case SMALLINT:
                    result = decodeAndCompareSmallInt(srcdata[i - 6], fds);
                    if (!result) {
                        m_rowFailed = true;
                        return false;
                    }
                    break;
                case INTEGER:
                    result = decodeAndCompareInteger(srcdata[i - 6], fds);
                    if (!result) {
                        m_rowFailed = true;
                        return false;
                    }
                    break;
                case BIGINT:
                    result = decodeAndCompareBigInt(srcdata[i - 6], fds);
                    if (!result) {
                        m_rowFailed = true;
                        return false;
                    }
                    break;
                case FLOAT:
                    result = decodeAndCompareFloat(srcdata[i - 6], fds);
                    if (!result) {
                        m_rowFailed = true;
                        return false;
                    }
                    break;
                case TIMESTAMP:
                    result = decodeAndCompareTimestamp(srcdata[i - 6], fds);
                    if (!result) {
                        m_rowFailed = true;
                        return false;
                    }
                    break;
                case STRING:
                    result = decodeAndCompareString(srcdata[i - 6], fds);
                    if (!result) {
                        m_rowFailed = true;
                        return false;
                    }
                    break;
                case DECIMAL:
                    result = decodeAndCompareDecimal(srcdata[i - 6], fds);
                    if (!result) {
                        m_rowFailed = true;
                        return false;
                    }
                    break;
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

    /**
     * Read a decimal according to the ELT encoding specification.
     * @param object expected result
     * @param fds Fastdeserializer containing ELT stream data
     * @return true if deserialized data equals expected result
     * @throws IOException
     */
    private boolean decodeAndCompareDecimal(final Object object,
                                            final FastDeserializer fds) throws IOException {
        if (object instanceof BigDecimal) {
            final BigDecimal bd1 = (BigDecimal)object;
            final BigDecimal bd2 = ELTDecoderBase.decodeDecimal(fds);
            return bd1.equals(bd2);
        }
        return false;
    }

    /**
     * Read a string according to the ELT encoding specification
     * @param object
     * @param fds
     * @return
     * @throws IOException
     */
    private boolean decodeAndCompareString(final Object object,
                                           final FastDeserializer fds) throws IOException {
        final String str = ELTDecoderBase.decodeString(fds);
        if (!((String)object).equals(str))
        {
            System.out.println("compare STRING failed: " + (String)object + " != " + str);
        }
        return ((String)object).equals(str);
    }

    /**
     * Read a timestamp according to the ELT encoding specification.
     * @param object
     * @param fds
     * @return
     * @throws IOException
     */
    private boolean decodeAndCompareTimestamp(final Object object,
                                              final FastDeserializer fds) throws IOException {
        final TimestampType ts = (TimestampType)object;
        final TimestampType dateval = ELTDecoderBase.decodeTimestamp(fds);
        if (ts.compareTo(dateval) != 0)
        {
            System.out.println("compare TIMESTAMP failed: " + ts + " != " + dateval);
        }
        return ts.compareTo(dateval) == 0;
    }

    /**
     * Read a float according to the ELT encoding specification
     * @param object
     * @param fds
     * @return
     * @throws IOException
     */
    private boolean decodeAndCompareFloat(final Object object,
                                          final FastDeserializer fds) throws IOException {
        final Double flt1 = (Double)object;
        final Double flt2 = ELTDecoderBase.decodeFloat(fds);
        if (flt1.compareTo(flt2) != 0)
        {
            System.out.println("compare FLOAT failed: " + flt1 + " != " + flt2);
        }
        return flt1.compareTo(flt2) == 0;
    }

    /**
     * Read a bigint according to the ELT encoding specification.
     * @param object
     * @param fds
     * @return
     * @throws IOException
     */
    private boolean decodeAndCompareBigInt(final Object object,
                                           final FastDeserializer fds) throws IOException {
        final Long l1 = (Long)object;
        final Long l2 = ELTDecoderBase.decodeBigInt(fds);
        if (l1.compareTo(l2) != 0)
        {
            System.out.println("compare BIGINT failed: " + l1 + " != " + l2);
        }
        return l1.compareTo(l2) == 0;
    }

    /**
     * Read an integer according to the ELT encoding specification.
     * @param object
     * @param fds
     * @return
     * @throws IOException
     */
    private boolean decodeAndCompareInteger(final Object object,
                                            final FastDeserializer fds) throws IOException {
        final Integer i1 = (Integer)object;
        final int i2 = ELTDecoderBase.decodeInteger(fds);
        if (i1.intValue() != i2)
        {
            System.out.println("compare INTEGER failed: " + i1 + " != " + i2);
        }
        return i1.intValue() == i2;
    }

    /**
     * Read a small int according to the ELT encoding specification.
     * @param object
     * @param fds
     * @return
     * @throws IOException
     */
    private boolean decodeAndCompareSmallInt(final Object object,
                                             final FastDeserializer fds) throws IOException {
        final Short a = (Short)object;
        final short b = ELTDecoderBase.decodeSmallInt(fds);
        if (a.shortValue() != b)
        {
            System.out.println("compare SMALLINT failed: " + a + " != " + b);
        }
        return a.shortValue() == b;
    }

    /**
     * Read a tiny int according to the ELT encoding specification.
     * @param object
     * @param fds
     * @return
     * @throws IOException
     */
    private boolean decodeAndCompareTinyInt(final Object object,
                                            final FastDeserializer fds) throws IOException {
        final Byte a = (Byte)object;
        final byte b = ELTDecoderBase.decodeTinyInt(fds);
        if (a.byteValue() != b)
        {
            System.out.println("compare TINYINT failed: " + a + " != " + b);
        }
        return a.byteValue() == b;
    }
}
