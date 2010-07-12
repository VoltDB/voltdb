/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.hsqldb_voltpatches.lib.StringUtil;

// fredt@users 20020414 - patch 828957 by tjcrowder@users - JDK 1.3 compatibility

/**
 * collection of static methods to convert Date and Timestamp strings
 * into corresponding Java objects and perform other Calendar related
 * operation.<p>
 *
 * Was reviewed for 1.7.2 resulting in centralising all DATETIME related
 * operstions.<p>
 *
 * From version 1.9.0, HSQLDB supports TIME ZONE with datetime types. The
 * values are stored internally as UTC seconds from 1970, regardless of the
 * time zone of the JVM, and converted as and when required, to the local
 * timezone.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.0
 */
public class HsqlDateTime {

    /**
     * A reusable static value for today's date. Should only be accessed
     * by getToday()
     */
    private static long           currentDateMillis;
    private static final Calendar tempCalDefault = new GregorianCalendar();
    public static final Calendar tempCalGMT =
        new GregorianCalendar(TimeZone.getTimeZone("GMT"));
    private static final Date   tempDate        = new Date(0);
    private static final String sdfdPattern     = "yyyy-MM-dd";
    static SimpleDateFormat     sdfd = new SimpleDateFormat(sdfdPattern);
    private static final String sdftPattern     = "HH:mm:ss";
    static SimpleDateFormat     sdft = new SimpleDateFormat(sdftPattern);
    private static final String sdftsPattern    = "yyyy-MM-dd HH:mm:ss";
    static SimpleDateFormat     sdfts = new SimpleDateFormat(sdftsPattern);
    private static final String sdftsSysPattern = "yyyy-MM-dd HH:mm:ss.SSS";
    static SimpleDateFormat sdftsSys = new SimpleDateFormat(sdftsSysPattern);

    static {
        tempCalGMT.setLenient(false);
        sdfd.setCalendar(new GregorianCalendar(TimeZone.getTimeZone("GMT")));
        sdfd.setLenient(false);
        sdft.setCalendar(new GregorianCalendar(TimeZone.getTimeZone("GMT")));
        sdft.setLenient(false);
        sdfts.setCalendar(new GregorianCalendar(TimeZone.getTimeZone("GMT")));
        sdfts.setLenient(false);
    }

    static {
        currentDateMillis = getNormalisedDate(System.currentTimeMillis());
    }

    public static long getDateSeconds(String s) {

        try {
            synchronized (sdfd) {
                java.util.Date d = sdfd.parse(s);

                return d.getTime() / 1000;
            }
        } catch (Exception e) {
            throw Error.error(ErrorCode.X_22007);
        }
    }

    public static String getDateString(long seconds) {

        synchronized (sdfd) {
            sysDate.setTime(seconds * 1000);

            return sdfd.format(sysDate);
        }
    }

    public static long getTimestampSeconds(String s) {

        try {
            synchronized (sdfts) {
                java.util.Date d = sdfts.parse(s);

                return d.getTime() / 1000;
            }
        } catch (Exception e) {
            throw Error.error(ErrorCode.X_22007);
        }
    }

    public static void getTimestampString(StringBuffer sb, long seconds,
                                          int nanos, int scale) {

        synchronized (sdfts) {
            tempDate.setTime(seconds * 1000);
            sb.append(sdfts.format(tempDate));

            if (scale > 0) {
                sb.append('.');
                sb.append(StringUtil.toZeroPaddedString(nanos, 9, scale));
            }
        }
    }

    public static String getTimestampString(long millis) {

        synchronized (sdfts) {
            sysDate.setTime(millis);

            return sdfts.format(sysDate);
        }
    }

    public static synchronized long getCurrentDateMillis(long millis) {

        if (millis - currentDateMillis >= 24 * 3600 * 1000) {
            currentDateMillis = getNormalisedDate(millis);
        }

        return currentDateMillis;
    }

    private static java.util.Date sysDate = new java.util.Date();

    public static String getSytemTimeString() {

        synchronized (sdftsSys) {
            sysDate.setTime(System.currentTimeMillis());

            return sdftsSys.format(sysDate);
        }
    }

    public static void resetToDate(Calendar cal) {

        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
    }

    public static void resetToTime(Calendar cal) {

        cal.set(Calendar.YEAR, 1970);
        cal.set(Calendar.MONTH, 0);
        cal.set(Calendar.DATE, 1);
        cal.set(Calendar.MILLISECOND, 0);
    }

    /**
     * Sets the time in the given Calendar using the given milliseconds value; wrapper method to
     * allow use of more efficient JDK1.4 method on JDK1.4 (was protected in earlier versions).
     *
     * @param       cal                             the Calendar
     * @param       millis                  the time value in milliseconds
     */
    public static void setTimeInMillis(Calendar cal, long millis) {

//#ifdef JAVA4
        // Use method directly
        cal.setTimeInMillis(millis);

//#else
/*
        // Have to go indirect
        synchronized (tempDate) {
            tempDate.setTime(millis);
            cal.setTime(tempDate);
        }
*/

//#endif JAVA4
    }

    /**
     * Gets the time from the given Calendar as a milliseconds value; wrapper method to
     * allow use of more efficient JDK1.4 method on JDK1.4 (was protected in earlier versions).
     *
     * @param       cal                             the Calendar
     * @return      the time value in milliseconds
     */
    public static long getTimeInMillis(Calendar cal) {

//#ifdef JAVA4
        // Use method directly
        return cal.getTimeInMillis();

//#else
/*
        // Have to go indirect
        return cal.getTime().getTime();
*/

//#endif JAVA4
    }

    public static long convertToNormalisedTime(long t) {
        return convertToNormalisedTime(t, tempCalGMT);
    }

    public static long convertToNormalisedTime(long t, Calendar cal) {

        synchronized (cal) {
            setTimeInMillis(cal, t);
            resetToDate(cal);

            long t1 = getTimeInMillis(cal);

            return t - t1;
        }
    }

    public static long convertToNormalisedDate(long t, Calendar cal) {

        synchronized (cal) {
            setTimeInMillis(cal, t);
            resetToDate(cal);

            return getTimeInMillis(cal);
        }
    }

    public static long getNormalisedTime(long t) {

        Calendar cal = tempCalGMT;

        synchronized (cal) {
            setTimeInMillis(cal, t);
            resetToTime(cal);

            return getTimeInMillis(cal);
        }
    }

    public static long getNormalisedDate(long d) {

        synchronized (tempCalGMT) {
            setTimeInMillis(tempCalGMT, d);
            resetToDate(tempCalGMT);

            return getTimeInMillis(tempCalGMT);
        }
    }

    public static int getZoneSeconds(Calendar cal) {
        return (cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET))
               / 1000;
    }

    public static int getZoneMillis(Calendar cal, long millis) {

//#ifdef JAVA4
        // get zone for the specific date
        return cal.getTimeZone().getOffset(millis);

//#else
/*
        // get zone for the specific date
        setTimeInMillis(cal, millis);
        return (cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET) );
*/

//#endif JAVA4
    }

    /**
     * Returns the indicated part of the given <code>java.util.Date</code> object.
     * @param m the millisecond time value from which to extract the indicated part
     * @param part an integer code corresponding to the desired date part
     * @return the indicated part of the given <code>java.util.Date</code> object
     */
    public static int getDateTimePart(long m, int part) {

        synchronized (tempCalGMT) {
            tempCalGMT.setTimeInMillis(m);

            return tempCalGMT.get(part);
        }
    }

    //J-

    private static final char[][] dateTokens     = {
        { 'R', 'R', 'R', 'R' }, { 'I', 'Y', 'Y', 'Y' }, { 'Y', 'Y', 'Y', 'Y' },
        { 'I', 'Y' }, { 'Y', 'Y' },
        { 'B', 'C' }, { 'B', '.', 'C', '.' }, { 'A', 'D' }, { 'A', '.', 'D', '.' },
        { 'M', 'O', 'N' }, { 'M', 'O', 'N', 'T', 'H' },
        { 'D', 'A', 'Y' }, { 'D', 'Y' },
        { 'I', 'W' }, { 'D', 'D' }, { 'D', 'D', 'D' },
        { 'H', 'H', '2', '4' }, { 'H', 'H', '1', '2' }, { 'H', 'H' },
        { 'M', 'I' },
        { 'S', 'S' },
        { 'A', 'M' }, { 'P', 'M' }, { 'A', '.', 'M', '.' }, { 'P', '.', 'M', '.' },
        { 'F', 'F' }
    };

    private static final String[] javaDateTokens = {
        "yyyy", "yyyy", "yyyy",
        "yy", "yy",
        "G", "G", "G", "G",
        "MMM", "MMMMM",
        "EEEE", "EE",
        "w", "dd", "D",
        "k", "K", "K",
        "mm", "ss",
        "aaa", "aaa", "aaa", "aaa",
        "S"
    };

    //J+

    /** Indicates end-of-input */
    private static final char e = 0xffff;

    /**
     * Converts the given format into a pattern accepted by <code>java.text.SimpleDataFormat</code>
     *
     * @param format
     */
    public static String toJavaDatePattern(String format) {

        int          len = format.length();
        char         ch;
        StringBuffer sb        = new StringBuffer(len);
        Tokenizer    tokenizer = new Tokenizer();

        for (int i = 0; i <= len; i++) {
            ch = (i == len) ? e
                            : format.charAt(i);

            if (!tokenizer.next(ch, dateTokens)) {
                int index = tokenizer.getLastMatch();

                if (index >= 0) {
                    sb.setLength(sb.length() - tokenizer.length());
                    sb.append(javaDateTokens[index]);
                }

                tokenizer.reset();

                if (tokenizer.isConsumed()) {
                    continue;
                }
            }

            sb.append(ch);
        }

        sb.setLength(sb.length() - 1);

        String javaPattern = sb.toString();

        return javaPattern;
    }

    /**
     * This class can match 64 tokens at maximum.
     */
    static class Tokenizer {

        private int     last;
        private int     offset;
        private long    state;
        private boolean consumed;

        public Tokenizer() {
            reset();
        }

        /**
         * Resets for next reuse.
         *
         */
        public void reset() {

            last   = -1;
            offset = -1;
            state  = 0;
        }

        /**
         * Returns a length of a token to match.
         */
        public int length() {
            return offset;
        }

        /**
         * Returns an index of the last matched token.
         */
        public int getLastMatch() {
            return last;
        }

        /**
         * Indicates whethe the last character has been consumed by the matcher.
         */
        public boolean isConsumed() {
            return consumed;
        }

        /**
         * Checks whether the specified bit is not set.
         *
         * @param bit
         */
        private boolean isZeroBit(int bit) {
            return (state & (1L << bit)) == 0;
        }

        /**
         * Sets the specified bit.
         * @param bit
         */
        private void setBit(int bit) {
            state |= (1L << bit);
        }

        /**
         * Matches the specified character against tokens.
         *
         * @param ch
         * @param tokens
         */
        public boolean next(char ch, char[][] tokens) {

            // Use local variable for performance
            int index = ++offset;
            int len   = offset + 1;
            int left  = 0;

            consumed = false;

            for (int i = tokens.length; --i >= 0; ) {
                if (isZeroBit(i)) {
                    if (tokens[i][index] == ch) {
                        consumed = true;

                        if (tokens[i].length == len) {
                            setBit(i);

                            last = i;
                        } else {
                            ++left;
                        }
                    } else {
                        setBit(i);
                    }
                }
            }

            return left > 0;
        }
    }
}
