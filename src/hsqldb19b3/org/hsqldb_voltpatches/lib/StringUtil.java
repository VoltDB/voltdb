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


package org.hsqldb_voltpatches.lib;

import java.lang.reflect.Array;

/** Provides a collection of convenience methods for processing and
 * creating objects with <code>String</code> value components.
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @author Nitin Chauhan
 * @version 1.9.0
 * @since 1.7.0
 */
public class StringUtil {

    /**
     * If necessary, adds zeros to the beginning of a value so that the total
     * length matches the given precision, otherwise trims the right digits.
     * Then if maxSize is smaller than precision, trims the right digits to
     * maxSize. Negative values are treated as positive
     */
    public static String toZeroPaddedString(long value, int precision,
            int maxSize) {

        StringBuffer sb = new StringBuffer();

        if (value < 0) {
            value = -value;
        }

        String s = Long.toString(value);

        if (s.length() > precision) {
            s = s.substring(precision);
        }

        for (int i = s.length(); i < precision; i++) {
            sb.append('0');
        }

        sb.append(s);

        if (maxSize < precision) {
            sb.setLength(maxSize);
        }

        return sb.toString();
    }

    public static String toPaddedString(String source, int length, char pad,
                                        boolean trailing) {

        int len = source.length();

        if (len >= length) {
            return source;
        }

        StringBuffer sb = new StringBuffer(length);

        if (trailing) {
            sb.append(source);
        }

        for (int i = len; i < length; i++) {
            sb.append(pad);
        }

        if (!trailing) {
            sb.append(source);
        }

        return sb.toString();
    }

    /**
     * Returns a string with non alphanumeric chars converted to the
     * substitute character. A digit first character is also converted.
     * By sqlbob@users
     * @param source string to convert
     * @param substitute character to use
     * @return converted string
     */
    public static String toLowerSubset(String source, char substitute) {

        int          len = source.length();
        StringBuffer sb = new StringBuffer(len);
        char         ch;

        for (int i = 0; i < len; i++) {
            ch = source.charAt(i);

            if (!Character.isLetterOrDigit(ch)) {
                sb.append(substitute);
            } else if ((i == 0) && Character.isDigit(ch)) {
                sb.append(substitute);
            } else {
                sb.append(Character.toLowerCase(ch));
            }
        }

        return sb.toString();
    }

    /**
     * Builds a bracketed CSV list from the array
     * @param array an array of Objects
     * @return string
     */
    public static String arrayToString(Object array) {

        int          len  = Array.getLength(array);
        int          last = len - 1;
        StringBuffer sb   = new StringBuffer(2 * (len + 1));

        sb.append('{');

        for (int i = 0; i < len; i++) {
            sb.append(Array.get(array, i));

            if (i != last) {
                sb.append(',');
            }
        }

        sb.append('}');

        return sb.toString();
    }

    /**
     * Builds a CSV list from the specified String[], separator string and
     * quote string. <p>
     *
     * <ul>
     * <li>All arguments are assumed to be non-null.
     * <li>Separates each list element with the value of the
     * <code>separator</code> argument.
     * <li>Prepends and appends each element with the value of the
     *     <code>quote</code> argument.
     * <li> No attempt is made to escape the quote character sequence if it is
     *      found internal to a list element.
     * <ul>
     * @return a CSV list
     * @param separator the <code>String</code> to use as the list element separator
     * @param quote the <code>String</code> with which to quote the list elements
     * @param s array of <code>String</code> objects
     */
    public static String getList(String[] s, String separator, String quote) {

        int          len = s.length;
        StringBuffer sb   = new StringBuffer(len * 16);

        for (int i = 0; i < len; i++) {
            sb.append(quote);
            sb.append(s[i]);
            sb.append(quote);

            if (i + 1 < len) {
                sb.append(separator);
            }
        }

        return sb.toString();
    }

    /**
     * Builds a CSV list from the specified int[], <code>separator</code>
     * <code>String</code> and <code>quote</code> <code>String</code>. <p>
     *
     * <ul>
     * <li>All arguments are assumed to be non-null.
     * <li>Separates each list element with the value of the
     * <code>separator</code> argument.
     * <li>Prepends and appends each element with the value of the
     *     <code>quote</code> argument.
     * <ul>
     * @return a CSV list
     * @param s the array of int values
     * @param separator the <code>String</code> to use as the separator
     * @param quote the <code>String</code> with which to quote the list elements
     */
    public static String getList(int[] s, String separator, String quote) {

        int          len = s.length;
        StringBuffer sb   = new StringBuffer(len * 8);

        for (int i = 0; i < len; i++) {
            sb.append(quote);
            sb.append(s[i]);
            sb.append(quote);

            if (i + 1 < len) {
                sb.append(separator);
            }
        }

        return sb.toString();
    }

    /**
     * Builds a CSV list from the specified String[][], separator string and
     * quote string. <p>
     *
     * <ul>
     * <li>All arguments are assumed to be non-null.
     * <li>Uses only the first element in each subarray.
     * <li>Separates each list element with the value of the
     * <code>separator</code> argument.
     * <li>Prepends and appends each element with the value of the
     *     <code>quote</code> argument.
     * <li> No attempt is made to escape the quote character sequence if it is
     *      found internal to a list element.
     * <ul>
     * @return a CSV list
     * @param separator the <code>String</code> to use as the list element separator
     * @param quote the <code>String</code> with which to quote the list elements
     * @param s the array of <code>String</code> array objects
     */
    public static String getList(String[][] s, String separator,
                                 String quote) {

        int          len = s.length;
        StringBuffer sb   = new StringBuffer(len * 16);

        for (int i = 0; i < len; i++) {
            sb.append(quote);
            sb.append(s[i][0]);
            sb.append(quote);

            if (i + 1 < len) {
                sb.append(separator);
            }
        }

        return sb.toString();
    }

    /**
     * Appends a pair of string to the string buffer, using the separator between
     * and terminator at the end
     * @param b the buffer
     * @param s1 first string
     * @param s2 second string
     * @param separator separator string
     * @param terminator terminator string
     */
    public static void appendPair(StringBuffer b, String s1, String s2,
                                  String separator, String terminator) {

        b.append(s1);
        b.append(separator);
        b.append(s2);
        b.append(terminator);
    }

    /**
     * Checks if text is empty (characters <= space)
     * @return boolean true if text is null or empty, false otherwise
     * @param s java.lang.String
     */
    public static boolean isEmpty(String s) {

        int i = s == null ? 0
                          : s.length();

        while (i > 0) {
            if (s.charAt(--i) > ' ') {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns the size of substring that does not contain any trailing spaces
     * @param s the string
     * @return trimmed size
     */
    public static int rightTrimSize(String s) {

        int i = s.length();

        while (i > 0) {
            i--;

            if (s.charAt(i) != ' ') {
                return i + 1;
            }
        }

        return 0;
    }

    /**
     * Skips any spaces at or after start and returns the index of first
     * non-space character;
     * @param s the string
     * @param start index to start
     * @return index of first non-space
     */
    public static int skipSpaces(String s, int start) {

        int limit = s.length();
        int i     = start;

        for (; i < limit; i++) {
            if (s.charAt(i) != ' ') {
                break;
            }
        }

        return i;
    }

    /**
     * Splits the string into an array, using the separator. If separator is
     * not found in the string, the whole string is returned in the array.
     *
     * @param s the string
     * @param separator the separator
     * @return array of strings
     */
    public static String[] split(String s, String separator) {

        HsqlArrayList list      = new HsqlArrayList();
        int           currindex = 0;

        for (boolean more = true; more; ) {
            int nextindex = s.indexOf(separator, currindex);

            if (nextindex == -1) {
                nextindex = s.length();
                more      = false;
            }

            list.add(s.substring(currindex, nextindex));

            currindex = nextindex + separator.length();
        }

        return (String[]) list.toArray(new String[list.size()]);
    }
}
