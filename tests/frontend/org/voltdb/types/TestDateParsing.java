/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.voltdb.types;

import java.util.Calendar;

import org.voltdb.parser.SQLParser;

import junit.framework.TestCase;

public class TestDateParsing extends TestCase {
    private void validateCalendar(Calendar now,
                                  int  year,
                                  int  month,
                                  int  dom,
                                  int  hour,
                                  int  minute,
                                  int  second,
                                  int  millis) {
        assertEquals(year, now.get(Calendar.YEAR));
        // Month is zero origin.
        assertEquals("Month:",        month-1, now.get(Calendar.MONTH));
        assertEquals("Day of Month:", dom, now.get(Calendar.DATE));
        assertEquals("Hour:",         hour, now.get(Calendar.HOUR));
        assertEquals("Minute:",       minute, now.get(Calendar.MINUTE));
        assertEquals("Second:",       second, now.get(Calendar.SECOND));
        // We assert that we use microseconds, but we really
        // only get milliseconds.
        assertEquals(millis, now.get(Calendar.MILLISECOND));
    }

    public void validateMicrosecondTime(long timestamp,
                                        int  year,
                                        int  month,
                                        int  dom,
                                        int  hour,
                                        int  minute,
                                        int  second,
                                        int  fractionalMicroSeconds) {
        long usec = timestamp % 1000000;
        long seconds = timestamp / 1000000;
        Calendar now = Calendar.getInstance();
        now.setTimeInMillis(seconds * 1000);
        validateCalendar(now, year, month, dom, hour, minute, second, 0);
        assertEquals(fractionalMicroSeconds, usec);
    }

    public long parseDate(String dateString)  {
        return SQLParser.parseDate(dateString).getTime();
    }

    public void parseDateFails(String dateString) {
        try {
            parseDate(dateString);
            assertFalse("Expected a date parsing error.", true);
        } catch (IllegalArgumentException ex) {
            assertTrue(true);
        }
    }

    public void testDateParsing() {
        TimestampType timestamp;
        timestamp = SQLParser.parseDate("2016-05-11 10:11:12.123");
        validateMicrosecondTime(timestamp.getTime(), 2016, 05, 11, 10, 11, 12, 123000);

        timestamp = SQLParser.parseDate("2016-05-11 10:11:12.12");
        validateMicrosecondTime(timestamp.getTime(), 2016, 05, 11, 10, 11, 12, 120000);

        timestamp = SQLParser.parseDate("2016-05-11 10:11:12.1");
        validateMicrosecondTime(timestamp.getTime(), 2016, 05, 11, 10, 11, 12, 100000);

        timestamp = SQLParser.parseDate("2016-05-11 10:11:12");
        validateMicrosecondTime(timestamp.getTime(), 2016, 05, 11, 10, 11, 12, 0);

        timestamp = SQLParser.parseDate("2016-05-11");
        validateMicrosecondTime(timestamp.getTime(), 2016, 05, 11, 0, 0, 0, 0);

        parseDateFails("20016-05-11");
        parseDateFails("16-05-11");
        // The month and day may only have 2 digits, not 1, and not
        // more than one.
        parseDateFails("2016-1-10");
        parseDateFails("2016-100-10");
        parseDateFails("2016-10-1");
        parseDateFails("2016-10-100");
        // The fractional seconds may have 1 through 6 digits but no more and no less.
        parseDateFails("2016-10-11 10:10:10.123456789");
        parseDateFails("2016-10-11 10:10:10.");
        // Tab characters are right out.
        parseDateFails("2016-10-11\t10:10:10.123456");
        // Only one fractional seconds field.
        parseDateFails("2016-10-11 10:10:10.123.456");
    }
}
