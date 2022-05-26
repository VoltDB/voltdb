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

package org.voltdb.client;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;


/**
 *   Creates a procedure invocation that has a paramter of each supported
 *   type and an array paramter of each supported type.  This is serialized
 *   and then deserialized as a stored procedure invocation (the server side's
 *   format) and the paramters are compared for equality (fidelity through the
 *   ser/deser process).
 */

public class TestProcedureInvocation extends TestCase{

    ProcedureInvocation pi;

    Byte byteparam;
    Short shortparam;
    Integer intparam;
    Long longparam;
    Double doubleparam;
    String stringparam;
    TimestampType dateparam;
    BigDecimal bigdecimalparam;
    VoltTable volttableparam;

    byte[] bytearray;
    short[] shortarray;
    int[] intarray;
    double[] doublearray;
    String[] stringarray;
    TimestampType[] datearray;
    BigDecimal[] bigdecimalarray;
    VoltTable[] volttablearray;

    @Override
    public void setUp() {
        byteparam = new Byte((byte) 2);
        shortparam = new Short(Short.MAX_VALUE);
        intparam = new Integer(Integer.MIN_VALUE);
        longparam = new Long(Long.MAX_VALUE -1);
        doubleparam = new Double(Double.MAX_VALUE -1);
        stringparam = new String("ABCDE");
        dateparam = new TimestampType(); // current time
        bigdecimalparam = new BigDecimal(7654321).setScale(VoltDecimalHelper.kDefaultScale);
        volttableparam = new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.INTEGER));
        volttableparam.addRow(Integer.MAX_VALUE);

        bytearray = new byte[] {(byte)'f', (byte)'o', (byte)'o'};
        shortarray = new short[] {Short.MAX_VALUE, Short.MIN_VALUE, (short)5};
        intarray = new int[] {Integer.MAX_VALUE, Integer.MIN_VALUE, 5};
        doublearray = new double[] {Double.MAX_VALUE, Double.MIN_VALUE, 5.5};
        stringarray = new String[] {"ABC", "DEF", "HIJ"};
        datearray = new TimestampType[] {new TimestampType(), new TimestampType(), new TimestampType()};

        BigDecimal bdtmp1 = new BigDecimal(7654321).setScale(VoltDecimalHelper.kDefaultScale);
        BigDecimal bdtmp2 = new BigDecimal(654321).setScale(VoltDecimalHelper.kDefaultScale);
        BigDecimal bdtmp3 = new BigDecimal(54321).setScale(VoltDecimalHelper.kDefaultScale);
        bigdecimalarray = new BigDecimal[] {bdtmp1, bdtmp2, bdtmp3};

        VoltTable vttmp1 = new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.INTEGER));
        vttmp1.addRow(Integer.MAX_VALUE);
        VoltTable vttmp2 = new VoltTable(new VoltTable.ColumnInfo("bar", VoltType.INTEGER));
        vttmp2.addRow(Integer.MIN_VALUE);
        VoltTable vttmp3 = new VoltTable(new VoltTable.ColumnInfo("far", VoltType.INTEGER));
        vttmp3.addRow(new Integer(5));
        volttablearray = new VoltTable[] { vttmp1, vttmp2, vttmp3 };

        assertTrue(bigdecimalparam.scale() == VoltDecimalHelper.kDefaultScale);
        assertTrue(bdtmp1.scale() == VoltDecimalHelper.kDefaultScale);
        assertTrue(bdtmp2.scale() == VoltDecimalHelper.kDefaultScale);
        assertTrue(bdtmp3.scale() == VoltDecimalHelper.kDefaultScale);


        pi = new ProcedureInvocation(10, "invocation1",
                                     byteparam, shortparam,
                                     intparam, longparam,
                                     doubleparam, stringparam,
                                     dateparam, bigdecimalparam,
                                     volttableparam,
                                     bytearray,
                                     shortarray,
                                     intarray,
                                     doublearray,
                                     stringarray,
                                     datearray,
                                     bigdecimalarray,
                                     volttablearray);
    }


    public void verifySpi(StoredProcedureInvocation spi) throws Exception {
        assertEquals(10, spi.getClientHandle());
        assertEquals(spi.getProcName(), "invocation1");
        assertEquals(spi.getParams().toArray()[0], byteparam);
        assertEquals(spi.getParams().toArray()[1], shortparam);
        assertEquals(spi.getParams().toArray()[2], intparam);
        assertEquals(spi.getParams().toArray()[3], longparam);
        assertEquals(spi.getParams().toArray()[4], doubleparam);
        assertEquals(spi.getParams().toArray()[5], stringparam);
        assertEquals(spi.getParams().toArray()[6], dateparam);
        assertEquals(spi.getParams().toArray()[7], bigdecimalparam);
        assertEquals(spi.getParams().toArray()[8], volttableparam);

        // this case is weird - byte arrays are converted to strings for the EE.
        // that conversion happens in the ParameterSet serialization and is evident
        // here.
        byte stringBytes[] = (byte[]) spi.getParams().toArray()[9];
        String bytestring = new String(stringBytes, "UTF-8");
        assertEquals(bytestring, "foo");

        short[] spishortarray = (short[]) spi.getParams().toArray()[10];
        assertEquals(spishortarray.length, 3);
        assertEquals(spishortarray.length, shortarray.length);
        for (int i=0; i < spishortarray.length; ++i)
            assertEquals(spishortarray[i], shortarray[i]);

        int[] spiintarray = (int[]) spi.getParams().toArray()[11];
        assertEquals(3, spiintarray.length);
        assertEquals(intarray.length, spiintarray.length);
        for (int i=0; i < intarray.length; ++i)
            assertEquals(intarray[i], spiintarray[i]);

        double[] spidoublearray = (double[]) spi.getParams().toArray()[12];
        assertEquals(3, spidoublearray.length);
        assertEquals(spidoublearray.length, doublearray.length);
        for (int i=0; i < spidoublearray.length; ++i)
            assertEquals(spidoublearray[i], doublearray[i]);

        String[] spistrarray = (String[]) spi.getParams().toArray()[13];
        assertEquals(3, spistrarray.length);
        assertEquals(spistrarray.length, stringarray.length);
        for (int i=0; i < spistrarray.length; ++i)
            assertEquals(spistrarray[i], stringarray[i]);

        TimestampType[] spidatearray = (TimestampType[]) spi.getParams().toArray()[14];
        assertEquals(3, spidatearray.length);
        assertEquals(spidatearray.length, datearray.length);
        for (int i=0; i < 3; i++)
            assertEquals(spidatearray[i], datearray[i]);

        BigDecimal[] spibdarray = (BigDecimal[]) spi.getParams().toArray()[15];
        assertEquals(3, spibdarray.length);
        assertEquals(3, bigdecimalarray.length);
        for (int i=0; i < 3; i++)
            assertEquals(spibdarray[i], bigdecimalarray[i]);

        VoltTable[] spivtarray = (VoltTable[]) spi.getParams().toArray()[16];
        assertEquals(3, spivtarray.length);
        assertEquals(3, volttablearray.length);
        for (int i=0; i < 3; i++)
            assertEquals(spivtarray[i], volttablearray[i]);
    }

    /** Mimic the de/ser path from client to client interface */
    public void testRoundTrip() throws Exception {
        assertEquals(10, pi.getHandle());
        ByteBuffer buf = ByteBuffer.allocate(pi.getSerializedSize());
        try {
            pi.flattenToBuffer(buf);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        buf.flip();

        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        try {
            spi.initFromBuffer(buf);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        verifySpi(spi);
    }

    public void testGetAsBytes() throws Exception {
        StoredProcedureInvocation spi = null;
        try {
            ByteBuffer buf = ByteBuffer.allocate(pi.getSerializedSize());
            pi.flattenToBuffer(buf);
            buf.flip();
            spi = new StoredProcedureInvocation();
            spi.initFromBuffer(buf);
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        verifySpi(spi);
    }
}
