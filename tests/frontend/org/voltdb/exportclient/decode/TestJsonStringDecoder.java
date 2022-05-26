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

import org.junit.Test;

public class TestJsonStringDecoder extends BaseForDecoderTests {

    final String expectedWithColumnNameXtion =
            "{\"tinyIntField\":10,\"smallIntField\":11,\"integerField\":12,"
          + "\"bigIntField\":13,\"floatField\":14.00014,\"timeStampField\":\"" + odbcDate
          + "\",\"stringField\":\"sixteen 十六\",\"varBinaryField\":\"" + base64Yolanda
          + "\",\"decimalField\":1818.0018,"
          + "\"geogPointField\":\"" + GEOG_POINT.toWKT()+ "\",\"geogField\":\"" + GEOG.toWKT() + "\"}";

    final String expectedWithoutColumnNameXtion=
            "{\"TINY_INT_FIELD\":10,\"SMALL_INT_FIELD\":11,\"INTEGER_FIELD\":12,"
                    + "\"BIG_INT_FIELD\":13,\"FLOAT_FIELD\":14.00014,\"TIME_STAMP_FIELD\":\"" + odbcDate
                    + "\",\"STRING_FIELD\":\"sixteen 十六\",\"VAR_BINARY_FIELD\":\"" + base64Yolanda
                    + "\",\"DECIMAL_FIELD\":1818.0018,"
                    + "\"GEOG_POINT_FIELD\":\"" + GEOG_POINT.toWKT()+ "\",\"GEOG_FIELD\":\"" + GEOG.toWKT() + "\"}";
    JsonStringDecoder.Builder builder = JsonStringDecoder.builder();

    @Test
    public void testDecodeRow() throws Exception {
        JsonStringDecoder decoder = builder.build();
        assertEquals(expectedWithColumnNameXtion, decoder.decode(0L, "mytable", TYPES, NAMES, null, row));
    }

    @Test
    public void testUmMangledRowNames() throws Exception {
        JsonStringDecoder decoder = builder.camelCaseFieldNames(false).build();
        assertEquals(expectedWithoutColumnNameXtion, decoder.decode(0L, "mytable", TYPES, NAMES, null, row));
    }

}
