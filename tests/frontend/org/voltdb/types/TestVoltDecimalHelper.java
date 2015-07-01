/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

    public void testSerializatinRoundTrip() throws Exception {
        BigDecimal bd = new BigDecimal("7654321");
        ByteBuffer buf = ByteBuffer.allocate(100);

        VoltDecimalHelper.serializeBigDecimal(bd, buf);
        buf.flip();
        BigDecimal bd2  = VoltDecimalHelper.deserializeBigDecimal(buf);

        System.out.println(bd.toString());
        System.out.println(bd2.toString());

        int cmp = bd.compareTo(bd2);
        assertEquals(0, cmp);
    }

    public void testSerializatinRoundTripNegative() throws Exception {
        BigDecimal bd = new BigDecimal("-23325.23425");
        ByteBuffer buf = ByteBuffer.allocate(100);

        VoltDecimalHelper.serializeBigDecimal(bd, buf);
        buf.flip();
        BigDecimal bd2  = VoltDecimalHelper.deserializeBigDecimal(buf);

        System.out.println(bd.toString());
        System.out.println(bd2.toString());

        int cmp = bd.compareTo(bd2);
        assertEquals(0, cmp);
    }

}
