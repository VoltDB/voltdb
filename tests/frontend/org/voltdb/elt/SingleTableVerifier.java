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
import java.nio.BufferUnderflowException;
import java.util.ArrayDeque;
import java.util.ArrayList;

import org.voltdb.VoltType;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.types.TimestampType;

/**
 * A tuple verifier that can verify a single table and assumes the data
 * arrives from VoltDB in same order as addRow() invocations.
 */
public class SingleTableVerifier implements TupleVerifier {
    private String m_tableName;
    private ArrayList<VoltType> m_tableSchema;
    private final ArrayDeque<Object[]> m_data;
    private boolean m_rowFailed = false;

    public SingleTableVerifier() {
        m_tableName = null;
        m_tableSchema = null;
        m_data = new ArrayDeque<Object[]>();
    }

    @Override
    synchronized public boolean allRowsVerified() {
        boolean result = (m_data.size() == 0) && (!m_rowFailed);
        if (!result) {
            System.out.println("SingleTableVerifier error. Data size: " +
                               m_data.size() + " row failed state: " + m_rowFailed);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    synchronized public void addTable(final String tableName,
                         final ArrayList<VoltType> tableSchema)
    {
        assert(m_tableName == null) : "May only add one table";
        assert(m_tableSchema == null) : "May only add one table schema.";
        m_tableName = tableName;
        m_tableSchema = (ArrayList<VoltType>) tableSchema.clone();
    }

    @Override
    synchronized public void addRow(final String tableName, final Object[] data) {
        // really tableName should match m_tableName.. but in reality
        // the ELTSuite uses multiple tables with identical column
        // schema but different names.
        m_data.offer(data);
    }

    @Override
    synchronized public void verifyRow(final int rowsize, final byte[] rowdata) throws IOException {
        boolean result = true;
        final FastDeserializer fds = new FastDeserializer(rowdata);
        final Object[] srcdata = m_data.poll();

        // no data found - an ERROR.
        if (srcdata == null) {
            m_rowFailed = true;
            return;
        }

        // otherwise, if an error was already registered, preserve that state
        if (m_rowFailed) {
            return;
        }

        // eltxxx: verify elt headers.
        // skip the null byte array - which length depends on the total column count
        // see EE's TupleStreamWrapper::computeOffsets()
        final int totalColCount = m_tableSchema.size() + 6;
        final int nullArrayLength = ((totalColCount + 7) & -8) >> 3;

        // skip the elt header (txnid, timestamp, seqno, pid, sid, 'I'/'D')
        fds.skipBytes(nullArrayLength);
        fds.skipBytes(41);

        // iterate the schema, decode and verify the rowdata
        try {
            for (int i=0; i < m_tableSchema.size(); i++) {
                switch (m_tableSchema.get(i)) {
                    default:
                        assert(false) : "Unknown type in verifyRow()";
                        result = false;
                        m_rowFailed = true;
                        return;
                    case TINYINT:
                        result = decodeAndCompareTinyInt(srcdata[i], fds);
                        if (!result) {
                            m_rowFailed = true;
                            return;
                        }
                        break;
                    case SMALLINT:
                        result = decodeAndCompareSmallInt(srcdata[i], fds);
                        if (!result) {
                            m_rowFailed = true;
                            return;
                        }
                        break;
                    case INTEGER:
                        result = decodeAndCompareInteger(srcdata[i], fds);
                        if (!result) {
                            m_rowFailed = true;
                            return;
                        }
                        break;
                    case BIGINT:
                        result = decodeAndCompareBigInt(srcdata[i], fds);
                        if (!result) {
                            m_rowFailed = true;
                            return;
                        }
                        break;
                    case FLOAT:
                        result = decodeAndCompareFloat(srcdata[i], fds);
                        if (!result) {
                            m_rowFailed = true;
                            return;
                        }
                        break;
                    case TIMESTAMP:
                        result = decodeAndCompareTimestamp(srcdata[i], fds);
                        if (!result) {
                            m_rowFailed = true;
                            return;
                        }
                        break;
                    case STRING:
                        result = decodeAndCompareString(srcdata[i], fds);
                        if (!result) {
                            m_rowFailed = true;
                            return;
                        }
                        break;
                    case DECIMAL:
                        result = decodeAndCompareDecimal(srcdata[i], fds);
                        if (!result) {
                            m_rowFailed = true;
                            return;
                        }
                        break;
                }
            }
        }
        catch (final BufferUnderflowException ex) {
            ex.printStackTrace();
            throw ex;
        }
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
        final int strlength = fds.readInt();
        final byte[] strdata = new byte[strlength];
        fds.readFully(strdata);
        final String str = new String(strdata);
        if (object instanceof BigDecimal) {
            final BigDecimal bd1 = (BigDecimal)object;
            try {
                final BigDecimal bd2 = new BigDecimal(str);
                return bd1.equals(bd2);
            } catch (Exception e) {
                System.out.println("error creating decimal from string(" + str + ")");
                e.printStackTrace();
            }
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
        final int strlength = fds.readInt();
        final byte[] strdata = new byte[strlength];
        fds.readFully(strdata);
        final String str = new String(strdata);
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
        final Long val = fds.readLong();
        final TimestampType dateval = new TimestampType(val);
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
        final Double flt2 = fds.readDouble();
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
        final Long l2 = fds.readLong();
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
        final Long i2 = fds.readLong();
        return i1.intValue() == i2.intValue();
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
        final Long b = fds.readLong();
        return a.longValue() == b.longValue();
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
        final Long b = fds.readLong();
        return a.byteValue() == b.byteValue();
    }
}
