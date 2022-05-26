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

import static org.voltdb.VoltType.BIGINT;
import static org.voltdb.VoltType.DECIMAL;
import static org.voltdb.VoltType.FLOAT;
import static org.voltdb.VoltType.GEOGRAPHY;
import static org.voltdb.VoltType.GEOGRAPHY_POINT;
import static org.voltdb.VoltType.INTEGER;
import static org.voltdb.VoltType.SMALLINT;
import static org.voltdb.VoltType.STRING;
import static org.voltdb.VoltType.TIMESTAMP;
import static org.voltdb.VoltType.TINYINT;
import static org.voltdb.VoltType.VARBINARY;
import static org.voltdb.exportclient.ExportDecoderBase.INTERNAL_FIELD_COUNT;

import java.math.BigDecimal;
import java.math.MathContext;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;

public class BaseForDecoderTests {

    final static List<String> NAMES = ImmutableList.copyOf(new String [] {
        "SKIP_1","SKIP_2","SKIP_3","SKIP_4","SKIP_5","SKIP_6",
        "TINY_INT_FIELD","SMALL_INT_FIELD","INTEGER_FIELD","BIG_INT_FIELD","FLOAT_FIELD",
        "TIME_STAMP_FIELD","STRING_FIELD","VAR_BINARY_FIELD", "DECIMAL_FIELD",
        "GEOG_POINT_FIELD", "GEOG_FIELD"
    });
    final static List<VoltType> TYPES = ImmutableList.copyOf(new VoltType[] {
        INTEGER,BIGINT,BIGINT,INTEGER,BIGINT,BIGINT,
        TINYINT,SMALLINT,INTEGER,BIGINT,FLOAT,
        TIMESTAMP,STRING,VARBINARY,DECIMAL,
        GEOGRAPHY_POINT, GEOGRAPHY
    });
    static final SimpleDateFormat odbcDateFmt = new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
    static final SimpleDateFormat pstOdbcDateFmt;
    static final String isoDateFormatString = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    static final SimpleDateFormat isoDateFmt = new SimpleDateFormat(isoDateFormatString);
    static final byte [] yolandaBytes = new String("YolandaBytes").getBytes(Charsets.UTF_8);
    static final String base64Yolanda = Encoder.base64Encode(yolandaBytes);
    static final String hexYolanda = Encoder.hexEncode(yolandaBytes);

    final static Map<VoltType,Integer> typeIndex;

    static {
        ImmutableMap.Builder<VoltType,Integer> mb = ImmutableMap.builder();
        for (int i = INTERNAL_FIELD_COUNT; i < TYPES.size(); ++i) {
            mb.put(TYPES.get(i),i);
        }
        typeIndex = mb.build();
        pstOdbcDateFmt = new SimpleDateFormat(Constants.ODBC_DATE_FORMAT_STRING);
        pstOdbcDateFmt.setTimeZone(TimeZone.getTimeZone("PST"));
    }

    final Date date = new Date();
    final String odbcDate = odbcDateFmt.format(date);
    final String pstOdbcDate = pstOdbcDateFmt.format(date);
    final String isoDate = isoDateFmt.format(date);
    final TimestampType timestamp = new TimestampType(date);

    static final GeographyPointValue GEOG_POINT = GeographyPointValue.fromWKT("point(-122 37)");
    static final GeographyValue GEOG = GeographyValue.fromWKT("polygon((0 0, 1 1, 0 1, 0 0))");

    final Object [] row = new Object[] {
        1,2L,3L,4,5L,6L,
        (byte)10,(short)11,12,13L,(double)14.00014,
        timestamp,"sixteen 十六",yolandaBytes,new BigDecimal(1818.0018, new MathContext(8)),
        GEOG_POINT, GEOG
    };

    public static String [] withoutInternals(final String [] withInternals) {
        return Arrays.copyOfRange(withInternals, INTERNAL_FIELD_COUNT, withInternals.length);
    }
}
