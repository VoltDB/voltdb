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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;
import static org.voltdb.VoltType.TIMESTAMP;
import static org.voltdb.VoltType.VARBINARY;
import static org.voltdb.exportclient.ExportDecoderBase.INTERNAL_FIELD_COUNT;

import java.util.Arrays;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.voltdb.common.Constants;
import org.voltdb.exportclient.ExportDecoderBase.BinaryEncoding;


public class TestStringArrayDecoder extends BaseForDecoderTests {

    StringArrayDecoder.Builder builder = new StringArrayDecoder.Builder();

    final String [] decoded = new String [] {
        "1","2","3","4","5","6",
        "10","11","12","13","14.00014",
        odbcDate,"sixteen 十六", base64Yolanda, "1818.0018",
        GEOG_POINT.toString(), GEOG.toWKT()
    };

    @Test
    public void testDecodeWithInternals() {
        builder.skipInternalFields(false);
        StringArrayDecoder sad = builder.build();

        assertThat(sad.decode(0L, "mytable", TYPES, NAMES, null, row), arrayContaining(decoded));
    }

    @Test
    public void testDecodeWithoutInternals() {
        builder.skipInternalFields(true);
        StringArrayDecoder sad = builder.build();
        assertThat(sad.decode(0L, "mytable", TYPES, NAMES, null, row), arrayContaining(withoutInternals(decoded)));
    }

    @Test
    public void testDifferentBinaryEncoding() {
        builder.binaryEncoding(BinaryEncoding.HEX);
        decoded[typeIndex.get(VARBINARY)] = hexYolanda;

        StringArrayDecoder sad = builder.build();
        assertThat(sad.decode(0L, "mytable", TYPES, NAMES, null, row), arrayContaining(withoutInternals(decoded)));
    }

    @Test
    public void testDifferentTimestampFormat() {
        builder.dateFormatter(isoDateFmt);
        decoded[typeIndex.get(TIMESTAMP)] = isoDate;

        StringArrayDecoder sad = builder.build();
        assertThat(sad.decode(0L, "mytable", TYPES, NAMES, null, row), arrayContaining(withoutInternals(decoded)));
    }

    @Test
    public void testDifferentTimeZone() {
        builder
            .dateFormatter(Constants.ODBC_DATE_FORMAT_STRING)
            .timeZone("PST");
        decoded[typeIndex.get(TIMESTAMP)] = pstOdbcDate;

        StringArrayDecoder sad = builder.build();
        assertThat(sad.decode(0L, "mytable", TYPES, NAMES, null, row), arrayContaining(withoutInternals(decoded)));
    }

    @Test
    public void testStringNullRepresentation() {
       builder.nullRepresentation("NIENTE");

       Arrays.fill(row, null);
       Arrays.fill(decoded,"NIENTE");

       StringArrayDecoder sad = builder.build();
       assertThat(sad.decode(0L, "mytable", TYPES, NAMES, null, row), arrayContaining(withoutInternals(decoded)));
     }

    @Test
    public void testNullNullRepresentation() {
       builder.nullRepresentation(null);

       Arrays.fill(row, null);
       Arrays.fill(decoded,(String)null);

       StringArrayDecoder sad = builder.build();
       assertThat(sad.decode(0L, "mytable", TYPES, NAMES, null, row), arrayContaining(withoutInternals(decoded)));
     }

    @Test
    public void testRowShorterThenInternalFields() {
        Object [] erow = Arrays.copyOfRange(row, 0, INTERNAL_FIELD_COUNT - 2);

        StringArrayDecoder sad = builder.build();
        try {
            sad.decode(0L, "mytable", TYPES, NAMES, null, erow);
            fail("row array size smaller than internal field count argument check");
        } catch (IllegalArgumentException iaex) {
            assertThat(iaex, messageContains("null or inapropriately sized export row array"));
        }
    }

    @Test
    public void testNullRow() {

        StringArrayDecoder sad = builder.build();
        try {
            sad.decode(0L, "mytable", TYPES, NAMES, null, null);
            fail("row array size smaller than internal field count");
        } catch (IllegalArgumentException iaex) {
            assertThat(iaex, messageContains("null or inapropriately sized export row array"));
        }
    }

    @Test
    public void testRowShorterThenExpected() {

        Object [] erow = Arrays.copyOfRange(row, 0, INTERNAL_FIELD_COUNT + 2);
        for (int i = INTERNAL_FIELD_COUNT + 2; i < decoded.length; ++i) {
            decoded[i] = null;
        }

        StringArrayDecoder sad = builder.build();
        assertThat(sad.decode(0L, "mytable", TYPES, NAMES, null, erow), arrayContaining(withoutInternals(decoded)));
    }

    final static Matcher<Exception> messageContains(final String portion) {
        return new TypeSafeMatcher<Exception>() {

            final Matcher<String> containsMatcher = containsString(portion);

            @Override
            public void describeTo(Description d) {
                d.appendText("<Exception [ message: ");
                d.appendDescriptionOf(containsMatcher);
                d.appendText(" ]>");
            }

            @Override
            protected boolean matchesSafely(Exception f) {
                return containsMatcher.matches(f.getMessage());
            }
        };
    }
}
