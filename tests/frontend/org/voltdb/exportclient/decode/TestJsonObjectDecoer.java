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

package org.voltdb.exportclient.decode;

import static org.junit.Assert.assertEquals;
import static org.voltdb.VoltType.BIGINT;
import static org.voltdb.VoltType.DECIMAL;
import static org.voltdb.VoltType.FLOAT;
import static org.voltdb.VoltType.INTEGER;
import static org.voltdb.VoltType.SMALLINT;
import static org.voltdb.VoltType.STRING;
import static org.voltdb.VoltType.TIMESTAMP;
import static org.voltdb.VoltType.TINYINT;
import static org.voltdb.VoltType.VARBINARY;

import java.math.BigDecimal;
import java.math.MathContext;

import org.json_voltpatches.JSONObject;
import org.junit.Test;
import org.voltdb.VoltType;

public class TestJsonObjectDecoer extends BaseForDecoderTests {

    final Object [] expected = new Object[] {
            1,2L,3L,4,5L,6L,
            10,11,12,13L,(double)14.00014,
            odbcDate,"sixteen 十六",base64Yolanda,new BigDecimal(1818.0018, new MathContext(8))
        };

    Object expectedFor(VoltType t) {
        return expected[typeIndex.get(t)];
    }

    @Test
    public void testStraightForwardDecode() throws Exception {
        JsonObjectDecoder.Builder builder = JsonObjectDecoder.builder();
        JsonObjectDecoder jod = builder.build();
        JSONObject jo = jod.decode(0L, "mytable", TYPES, NAMES, null, row);

        assertEquals(expectedFor(TIMESTAMP),jo.get("timeStampField"));
        assertEquals(expectedFor(DECIMAL),jo.get("decimalField"));
        assertEquals(expectedFor(BIGINT),jo.get("bigIntField"));
        assertEquals(expectedFor(FLOAT),jo.get("floatField"));
        assertEquals(expectedFor(TINYINT),jo.get("tinyIntField"));
        assertEquals(expectedFor(VARBINARY),jo.get("varBinaryField"));
        assertEquals(expectedFor(INTEGER),jo.get("integerField"));
        assertEquals(expectedFor(STRING),jo.get("stringField"));
        assertEquals(expectedFor(SMALLINT),jo.get("smallIntField"));
    }

    @Test
    public void testUnmangledFieldNames() throws Exception {
        JsonObjectDecoder.Builder builder = JsonObjectDecoder.builder();
        builder.camelCaseFieldNames(false);
        JsonObjectDecoder jod = builder.build();
        JSONObject jo = jod.decode(0L, "mytable", TYPES, NAMES, null, row);

        assertEquals(expectedFor(TIMESTAMP),jo.get("TIME_STAMP_FIELD"));
        assertEquals(expectedFor(DECIMAL),jo.get("DECIMAL_FIELD"));
        assertEquals(expectedFor(BIGINT),jo.get("BIG_INT_FIELD"));
        assertEquals(expectedFor(FLOAT),jo.get("FLOAT_FIELD"));
        assertEquals(expectedFor(TINYINT),jo.get("TINY_INT_FIELD"));
        assertEquals(expectedFor(VARBINARY),jo.get("VAR_BINARY_FIELD"));
        assertEquals(expectedFor(INTEGER),jo.get("INTEGER_FIELD"));
        assertEquals(expectedFor(STRING),jo.get("STRING_FIELD"));
        assertEquals(expectedFor(SMALLINT),jo.get("SMALL_INT_FIELD"));
    }
}
