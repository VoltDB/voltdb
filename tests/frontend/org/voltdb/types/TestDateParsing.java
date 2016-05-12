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
        // The month and day may only have 1 or 2 digits.
        parseDateFails("2016-100-10");
        parseDateFails("2016-10-100");
        // The fractional seconds may have zero through 6 digits but no more.
        parseDateFails("2016-10-11 10:10:10.123456789");
        // Tab characters are not right out.
        parseDateFails("2016-10-11\t10:10:10.123456");
    }
}
