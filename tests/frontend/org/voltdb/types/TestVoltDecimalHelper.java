/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

public class TestVoltDecimalHelper extends TestCase {

    public void testNullSerialization() {
        BigDecimal bd = null;
        ByteBuffer buf = ByteBuffer.allocate(100);
        VoltDecimalHelper.serializeBigDecimal(bd, buf);
        buf.flip();
        BigDecimal bd2 = VoltDecimalHelper.deserializeBigDecimal(buf);
        assertEquals(bd2, null);
    }

    public void testNullBigDecimalAsSQLNull() {
        ByteBuffer buf = ByteBuffer.allocate(100);
        VoltDecimalHelper.serializeNull(buf);
        buf.flip();
        BigDecimal bd2 = VoltDecimalHelper.deserializeBigDecimal(buf);
        assertEquals(bd2, null);
    }

    public void testSerializationRoundTrip() throws Exception {
        BigDecimal bd;
        ByteBuffer buf;
        BigDecimal bd2;
        int cmp;
        byte[] bytes;

        bd = new BigDecimal("7654321");
        buf = ByteBuffer.allocate(100);

        VoltDecimalHelper.serializeBigDecimal(bd, buf);
        buf.flip();
        bd2  = VoltDecimalHelper.deserializeBigDecimal(buf);

        //*enable to debug */ System.out.println(bd.toString());
        //*enable to debug */ System.out.println(bd2.toString());

        cmp = bd.compareTo(bd2);
        assertEquals(0, cmp);

        // Test max number of digits after decimal
        bd = new BigDecimal("3.210987654321");
        bytes = VoltDecimalHelper.serializeBigDecimal(bd);
        buf = ByteBuffer.wrap(bytes);
        bd2 = VoltDecimalHelper.deserializeBigDecimal(buf);
        cmp = bd.compareTo(bd2);
        assertEquals(0, cmp);


        // Test max number of total digits
        bd = new BigDecimal("65432109876543210987654321.210987654321");
        bytes = VoltDecimalHelper.serializeBigDecimal(bd);
        buf = ByteBuffer.wrap(bytes);
        bd2 = VoltDecimalHelper.deserializeBigDecimal(buf);
        cmp = bd.compareTo(bd2);
        assertEquals(0, cmp);


        // ENG-9630 -- problems with multiples of 10 initialized via string.

        bd = new BigDecimal("3.98336E7");
        bytes = VoltDecimalHelper.serializeBigDecimal(bd);
        buf = ByteBuffer.wrap(bytes);
        bd2 = VoltDecimalHelper.deserializeBigDecimal(buf);
        cmp = bd.compareTo(bd2);
        assertEquals(0, cmp);

        bd = new BigDecimal("9E25");
        bytes = VoltDecimalHelper.serializeBigDecimal(bd);
        buf = ByteBuffer.wrap(bytes);
        bd2 = VoltDecimalHelper.deserializeBigDecimal(buf);
        cmp = bd.compareTo(bd2);
        assertEquals(0, cmp);

    }

    public void testSerializationRoundTripNegative() throws Exception {
        BigDecimal bd = new BigDecimal("-23325.23425");
        ByteBuffer buf = ByteBuffer.allocate(100);

        VoltDecimalHelper.serializeBigDecimal(bd, buf);
        buf.flip();
        BigDecimal bd2  = VoltDecimalHelper.deserializeBigDecimal(buf);

        //*enable to debug */ System.out.println(bd.toString());
        //*enable to debug */ System.out.println(bd2.toString());

        int cmp = bd.compareTo(bd2);
        assertEquals(0, cmp);
    }

}
