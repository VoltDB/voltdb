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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


import java.text.SimpleDateFormat;
import java.util.Date;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

public class TestEndpointExpander {

    Date   date = new Date();
    String dateString = new SimpleDateFormat("yyyyMMdd'T'HH").format(date);

    @Test
    public void testFormatParameter() {
        try {
            EndpointExpander.expand(null, "table", 1, 12345678L, date);
            fail("format parameter check");
        } catch (IllegalArgumentException e) {
            assertThat(e,messageContains("null or empty format string"));
        }
        try {
            EndpointExpander.expand("    ", "table", 1, 12345678L, date);
            fail("format parameter check");
        } catch (IllegalArgumentException e) {
            assertThat(e,messageContains("null or empty format string"));
        }
    }

    @Test
    public void testTableNameParameter() {
        try {
            EndpointExpander.expand("http://t.c/%t_%v", null, 1, 12345678L, date);
            fail("table name parameter check");
        } catch (IllegalArgumentException e) {
            assertThat(e,messageContains("null or empty table name"));
        }
        try {
            EndpointExpander.expand("http://t.c/%t_%v", "    ", 1, 12345678L, date);
            fail("table name parameter check");
        } catch (IllegalArgumentException e) {
            assertThat(e,messageContains("null or empty table name"));
        }
    }

    @Test
    public void testDateParameter() {
        try {
            EndpointExpander.expand("http://t.c/%t_%p_%d", "table", 1, 12345678L, null);
            fail("date parameter check");
        } catch (IllegalArgumentException e) {
            assertThat(e,messageContains("null date"));
        }
    }

    @Test
    public void testExpansions() {
        String expanded;

        expanded = EndpointExpander.expand("h://t.c/%t/%p/%t_%p_%g_%d", "table",2,33,date);
        assertEquals("h://t.c/table/2/table_2_x_"+dateString, expanded);

        expanded = EndpointExpander.expand("h://t.c/%t/%p/%t_%p_%p_%g_%d", "table",2,33,date);
        assertEquals("h://t.c/table/2/table_2_2_x_"+dateString, expanded);

        expanded = EndpointExpander.expand("h://t.c/%t/%p/%t_%p_%%p_%%t", "table",2,33,date);
        assertEquals("h://t.c/table/2/table_2_%p_%t", expanded);

        expanded = EndpointExpander.expand("h://t.c/%t/%p/%t_%p_%%o_%o", "table",2,33,date);
        assertEquals("h://t.c/table/2/table_2_%o_%o", expanded);
    }

    @Test
    public void testDateConversionExistence() {
        assertTrue(EndpointExpander.hasDateConversion("h://t.c/%d"));
        assertFalse(EndpointExpander.hasDateConversion("h://t.c/%%d_%g"));
    }

    @Test
    public void testVerifyForHdfsUse() throws Exception {
        try {
            EndpointExpander.verifyForHdfsUse("h://t.c/d/f.c");
            fail("verify for hdfs");
        } catch (IllegalArgumentException e) {
            assertThat(e,messageContains("must contain"));
        }
        try {
            EndpointExpander.verifyForHdfsUse("h://t.c/d/%t_%p.c");
            fail("verify for hdfs");
        } catch (IllegalArgumentException e) {
            assertThat(e,messageContains("must contain"));
        }
        try {
            EndpointExpander.verifyForHdfsUse("h://t.c/%m/%t_%p_%g.c");
            fail("verify for hdfs");
        } catch (IllegalArgumentException e) {
            assertThat(e,messageContains("Malformed escape"));
        }
        try {
            EndpointExpander.verifyForHdfsUse("h://t.c/%p/%t.c?gen=%g&date=%d");
            fail("verify for hdfs");
        } catch (IllegalArgumentException e) {
            assertThat(e,messageContains("in the path element"));
        }
        EndpointExpander.verifyForHdfsUse("h://t.c/%g/%d/%p/%t.c?gen=%g&date=%d");
        try {
            EndpointExpander.verifyForHdfsUse("h://t.c/%g/%p/%t.c?gen=%g&date=%d");
            fail("verify for hdfs");
        } catch (IllegalArgumentException e) {
            assertThat(e,messageContains("in the path element"));
        }
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
