/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
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

import java.util.Locale;

import org.hsqldb_voltpatches.persist.HsqlDatabaseProperties;

// fredt@users 20020305 - patch 1.7.0 - change to 2D string arrays
// sqlbob@users 20020420- patch 1.7.0 - added HEXTORAW and RAWTOHEX.
// boucherb@user 20020918 - doc 1.7.2 - added JavaDoc  and code comments
// fredt@user 20021021 - doc 1.7.2 - modified JavaDoc
// boucherb@users 20030201 - patch 1.7.2 - direct calls for org.hsqldb_voltpatches.Library
// fredt@users - patch 1.8.0 - new functions added
// boucherb@users 20060428 - patch 1.8.0 - Bug fix for [ 1455637 ] allow double args for modulo
// fredt@users - patch 1.9.0 - most methods removed - see legacy support details

/**
 * fredt - since the introduction of SQL built-in functions and rewrite of
 * OpenGroup CLI functions as SQL functions, most previous methods have been
 * removed. The remaining methods are called when SQL functions are executed.
 */

/**
 * Provides the HSQLDB implementation of standard Open Group SQL CLI
 * <em>Extended Scalar Functions</em> and other public HSQLDB SQL functions.<p>
 *
 * All methods here that have a Connection parameter are dummies and should
 * not be called from user supplied Java procedure or trigger code. Use real
 * SQL functions should be called instead in these instances.
 *
 * For 1.9.0, several methods have been deleted. These methods are now supported
 * by other classes. All the deleted method signatures are still supported.
 *
 * Extensively rewritten and extended in successive versions of HSQLDB.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.8.0
 * @since Hypersonic SQL
 */
public class Library {

    private Library() {}

    /**
     * Retrieves a <em>magically</em> rounded </code>double</code> value produced
     * from the given <code>double</code> value.  This method provides special
     * handling for numbers close to zero and performs rounding only for
     * numbers within a specific range, returning  precisely the given value
     * if it does not lie in this range. <p>
     *
     * Special handling includes: <p>
     *
     * <UL>
     * <LI> input in the interval -0.0000000000001..0.0000000000001 returns 0.0
     * <LI> input outside the interval -1000000000000..1000000000000 returns
     *      input unchanged
     * <LI> input is converted to String form
     * <LI> input with a <code>String</code> form length greater than 16 returns
     *      input unchaged
     * <LI> <code>String</code> form with last four characters of '...000x' where
     *      x != '.' is converted to '...0000'
     * <LI> <code>String</code> form with last four characters of '...9999' is
     *      converted to '...999999'
     * <LI> the <code>java.lang.Double.doubleValue</code> of the <code>String</code>
     *      form is returned
     * </UL>
     * @param d the double value for which to retrieve the <em>magically</em>
     *      rounded value
     * @return the <em>magically</em> rounded value produced
     */
    public static double roundMagic(double d) {

        // this function rounds numbers in a good way but slow:
        // - special handling for numbers around 0
        // - only numbers <= +/-1000000000000
        // - convert to a string
        // - check the last 4 characters:
        // '000x' becomes '0000'
        // '999x' becomes '999999' (this is rounded automatically)
        if ((d < 0.0000000000001) && (d > -0.0000000000001)) {
            return 0.0;
        }

        if ((d > 1000000000000.) || (d < -1000000000000.)) {
            return d;
        }

        StringBuffer sb = new StringBuffer();

        sb.append(d);

        int len = sb.length();

        if (len < 16) {
            return d;
        }

        char cx = sb.charAt(len - 1);
        char c1 = sb.charAt(len - 2);
        char c2 = sb.charAt(len - 3);
        char c3 = sb.charAt(len - 4);

        if ((c1 == '0') && (c2 == '0') && (c3 == '0') && (cx != '.')) {
            sb.setCharAt(len - 1, '0');
        } else if ((c1 == '9') && (c2 == '9') && (c3 == '9') && (cx != '.')) {
            sb.setCharAt(len - 1, '9');
            sb.append('9');
            sb.append('9');
        }

        return Double.valueOf(sb.toString()).doubleValue();
    }

    /**
     * Returns the given <code>double</code> value, rounded to the given
     * <code>int</code> places right of the decimal point. If
     * the supplied rounding place value is negative, rounding is performed
     * to the left of the decimal point, using its magnitude (absolute value).
     * @param d the value to be rounded
     * @param p the rounding place value
     * @return <code>d</code> rounded
     */
    public static double round(double d, int p) {

        double f = Math.pow(10., p);

        return Math.round(d * f) / f;
    }

    // STRING FUNCTIONS

    /**
     * Returns the character string corresponding to the given ASCII
     * (or Unicode) value.
     *
     * <b>Note:</b> <p>
     *
     * In some SQL CLI
     * implementations, a <code>null</code> is returned if the range is outside 0..255.
     * In HSQLDB, the corresponding Unicode character is returned
     * unchecked.
     * @param code the character code for which to return a String
     *      representation
     * @return the String representation of the character
     */
    public static String character(int code) {
        return String.valueOf((char) code);
    }

    /**
     * Returns a count of the characters that do not match when comparing
     * the 4 digit numeric SOUNDEX character sequences for the
     * given <code>String</code> objects.  If either <code>String</code> object is
     * <code>null</code>, zero is returned.
     * @param s1 the first <code>String</code>
     * @param s2 the second <code>String</code>
     * @return the number of differences between the <code>SOUNDEX</code> of
     *      <code>s1</code> and the <code>SOUNDEX</code> of <code>s2</code>
     */

// fredt@users 20020305 - patch 460907 by fredt - soundex
    public static int difference(String s1, String s2) {

        /** @todo: check if this is the standard algorithm */
        if ((s1 == null) || (s2 == null)) {
            return 0;
        }

        s1 = soundex(s1);
        s2 = soundex(s2);

        int e = 0;

        for (int i = 0; i < 4; i++) {
            if (s1.charAt(i) != s2.charAt(i)) {
                e++;
            }
        }

        return e;
    }

    /**
     * Returns the starting position of the first occurrence of
     * the given <code>search</code> <code>String</code> object within
     * the given <code>String</code> object, <code>s</code>.
     *
     * The search for the first occurrence of <code>search</code> begins with
     * the first character position in <code>s</code>, unless the optional
     * argument, <code>start</code>, is specified (non-null). If
     * <code>start</code> is specified, the search begins with the character
     * position indicated by the value of <code>start</code>, where the
     * first character position in <code>s</code> is indicated by the value 1.
     * If <code>search</code> is not found within <code>s</code>, the
     * value 0 is returned.
     * @param search the <code>String</code> occurence to find in <code>s</code>
     * @param s the <code>String</code> within which to find the first
     *      occurence of <code>search</code>
     * @param start the optional character position from which to start
     *      looking in <code>s</code>
     * @return the one-based starting position of the first occurrence of
     *      <code>search</code> within <code>s</code>, or 0 if not found
     */
    public static int locate(String search, String s, Integer start) {

        if (s == null || search == null) {
            return 0;
        }

        int i = (start == null) ? 0
                                : start.intValue() - 1;

        return s.indexOf(search, (i < 0) ? 0
                                         : i) + 1;
    }

    /**
     * Returns a <code>String</code> composed of the given <code>String</code>,
     * repeated  <code>count</code> times.
     *
     * @param s the <code>String</code> to repeat
     * @param count the number of repetitions
     * @return the given <code>String</code>, repeated <code>count</code> times
     */
    public static String repeat(String s, Integer count) {

        if (s == null || count == null || count.intValue() < 0) {
            return null;
        }

        int          i  = count.intValue();
        StringBuffer sb = new StringBuffer(s.length() * i);

        while (i-- > 0) {
            sb.append(s);
        }

        return sb.toString();
    }

// fredt@users - 20020903 - patch 1.7.1 - bug fix to allow multiple replaces

    /**
     * Replaces all occurrences of <code>replace</code> in <code>s</code>
     * with the <code>String</code> object: <code>with</code>
     * @param s the target for replacement
     * @param replace the substring(s), if any, in <code>s</code> to replace
     * @param with the value to substitute for <code>replace</code>
     * @return <code>s</code>, with all occurences of <code>replace</code>
     *      replaced by <code>with</code>
     */
    public static String replace(String s, String replace, String with) {

        if (s == null || replace == null) {
            return s;
        }

        if (with == null) {
            with = "";
        }

        StringBuffer b          = new StringBuffer();
        int          start      = 0;
        int          lenreplace = replace.length();

        while (true) {
            int i = s.indexOf(replace, start);

            if (i == -1) {
                b.append(s.substring(start));

                break;
            }

            b.append(s.substring(start, i));
            b.append(with);

            start = i + lenreplace;
        }

        return b.toString();
    }

// fredt@users 20011010 - patch 460907 by fredt - soundex

    /**
     * Returns a four character code representing the sound of the given
     * <code>String</code>. Non-ASCCI characters in the
     * input <code>String</code> are ignored. <p>
     *
     * This method was
     * rewritten for HSQLDB by fredt@users to comply with the description at
     * <a href="http://www.archives.gov/genealogy/census/soundex.html">
     * http://www.archives.gov/genealogy/census/soundex.html </a>.<p>
     * @param s the <code>String</code> for which to calculate the 4 character
     *      <code>SOUNDEX</code> value
     * @return the 4 character <code>SOUNDEX</code> value for the given
     *      <code>String</code>
     */
    public static String soundex(String s) {

        if (s == null) {
            return s;
        }

        s = s.toUpperCase(Locale.ENGLISH);

        int    len       = s.length();
        char[] b         = new char[] {
            '0', '0', '0', '0'
        };
        char   lastdigit = '0';

        for (int i = 0, j = 0; i < len && j < 4; i++) {
            char c = s.charAt(i);
            char newdigit;

            if ("AEIOUY".indexOf(c) != -1) {
                newdigit = '7';
            } else if (c == 'H' || c == 'W') {
                newdigit = '8';
            } else if ("BFPV".indexOf(c) != -1) {
                newdigit = '1';
            } else if ("CGJKQSXZ".indexOf(c) != -1) {
                newdigit = '2';
            } else if (c == 'D' || c == 'T') {
                newdigit = '3';
            } else if (c == 'L') {
                newdigit = '4';
            } else if (c == 'M' || c == 'N') {
                newdigit = '5';
            } else if (c == 'R') {
                newdigit = '6';
            } else {
                continue;
            }

            if (j == 0) {
                b[j++]    = c;
                lastdigit = newdigit;
            } else if (newdigit <= '6') {
                if (newdigit != lastdigit) {
                    b[j++]    = newdigit;
                    lastdigit = newdigit;
                }
            } else if (newdigit == '7') {
                lastdigit = newdigit;
            }
        }

        return new String(b, 0, 4);
    }

    /**
     * Returns the characters from the given <code>String</code>, starting at
     * the indicated one-based <code>start</code> position and extending the
     * (optional) indicated <code>length</code>. If <code>length</code> is not
     * specified (is <code>null</code>), the remainder of <code>s</code> is
     * implied.
     *
     * The rules for boundary conditions on s, start and length are,
     * in order of precedence: <p>
     *
     * 1.) if s is null, return null
     *
     * 2.) If length is less than 1, return null.
     *
     * 3.) If start is 0, it is treated as 1.
     *
     * 4.) If start is positive, count from the beginning of s to find
     *     the first character postion.
     *
     * 5.) If start is negative, count backwards from the end of s
     *     to find the first character.
     *
     * 6.) If, after applying 2.) or 3.), the start position lies outside s,
     *     then return null
     *
     * 7.) if length is ommited or is greated than the number of characters
     *     from the start position to the end of s, return the remaineder of s,
     *     starting with the start position.
     *
     * @param s the <code>String</code> from which to produce the indicated
     *      substring
     * @param start the starting position of the desired substring
     * @param length the length of the desired substring
     * @return the indicted substring of <code>s</code>.
     */

    /**
     * Retrieves the full version number of this database product. <p>
     *
     * @return database version number as a <code>String</code> object
     * @since 1.8.0.4
     */
    public static String getDatabaseFullProductVersion() {
        return HsqlDatabaseProperties.THIS_FULL_VERSION;
    }

    /**
     * Retrieves the name of this database product. <p>
     *
     * @return database product name as a <code>String</code> object
     * @since 1.7.2
     */
    public static String getDatabaseProductName() {
        return HsqlDatabaseProperties.PRODUCT_NAME;
    }

    /**
     * Retrieves the version number of this database product. <p>
     *
     * @return database version number as a <code>String</code> object
     * @since 1.7.2
     */
    public static String getDatabaseProductVersion() {
        return HsqlDatabaseProperties.THIS_VERSION;
    }

    /**
     * Retrieves the major version number of this database. <p>
     *
     * @return the database's major version as an <code>int</code> value
     * @since 1.7.2
     */
    public static int getDatabaseMajorVersion() {
        return HsqlDatabaseProperties.MAJOR;
    }

    /**
     * Retrieves the major version number of this database. <p>
     *
     * @return the database's major version as an <code>int</code> value
     * @since 1.7.2
     */
    public static int getDatabaseMinorVersion() {
        return HsqlDatabaseProperties.MINOR;
    }
}
