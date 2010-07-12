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

import org.hsqldb_voltpatches.lib.StringUtil;
import org.hsqldb_voltpatches.resources.BundleHandler;
import org.hsqldb_voltpatches.result.Result;

/**
 * Contains static factory methods to produce instances of HsqlException
 *
 * @author Loic Lefevre
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class Error {

    //
    public static boolean TRACE          = false;
    public static boolean TRACESYSTEMOUT = false;

    //
    private static final String errPropsName = "sql-state-messages";
    private static final int bundleHandle =
        BundleHandler.getBundleHandle(errPropsName, null);
    private static final String MESSAGE_TAG      = "$$";
    private static final int    SQL_STATE_DIGITS = 5;
    private static final int    SQL_CODE_DIGITS  = 4;
    private static final int    ERROR_CODE_BASE  = 11;

    public static RuntimeException runtimeError(int code, String add) {

        HsqlException e = error(code, add);

        return new RuntimeException(e.getMessage());
    }

    public static HsqlException error(int code, String add) {

        String s = getMessage(code);

        if (add != null) {
            s += ": " + add.toString();
        }

        return new HsqlException(s.substring(SQL_STATE_DIGITS + 1),
                                 s.substring(0, SQL_STATE_DIGITS), -code);
    }

    public static HsqlException error(int code) {
        return error(code, 0, null);
    }

    public static HsqlException error(int code, Throwable t) {

        String message = getMessage(code, 0, null);

        return new HsqlException(t, message.substring(0, SQL_STATE_DIGITS),
                                 -code);
    }

    /**
     * Compose error message by inserting the strings in the add parameters
     * in placeholders within the error message. The message string contains
     * $$ markers for each context variable. Context variables are supplied in
     * the add parameters.
     *
     * @param code      main error code
     * @param subCode   sub error code (if 0 => no subMessage!)
     * @param   add     optional parameters
     *
     * @return an <code>HsqlException</code>
     */
    public static HsqlException error(int code, int subCode,
                                      final Object[] add) {

        String message = getMessage(code, subCode, add);
        int    sqlCode = subCode < ERROR_CODE_BASE ? code
                                                   : subCode;

        return new HsqlException(message.substring(SQL_STATE_DIGITS + 1),
                                 message.substring(0, SQL_STATE_DIGITS),
                                 -sqlCode);
    }

    public static HsqlException error(int code, int code2) {
        return error(code, getMessage(code2));
    }

    /**
     * Compose error
     * in placeholders within the error message. The message string contains
     * $$ markers for each context variable. Context variables are supplied in
     * the add parameters.
     *
     * @see HsqlException#HsqlException(String, String, int)
     * @return an <code>HsqlException</code>
     */
    public static HsqlException error(String message, String sqlState, int i) {
        return new HsqlException(message, sqlState, i);
    }

    /**
     * Compose error message by inserting the strings in the add variables
     * in placeholders within the error message. The message string contains
     * $$ markers for each context variable. Context variables are supplied in
     * the add parameter. (by Loic Lefevre)
     *
     * @param message  message string
     * @param add      optional parameters
     *
     * @return an <code>HsqlException</code>
     */
    private static String insertStrings(String message, Object[] add) {

        StringBuffer sb        = new StringBuffer(message.length() + 32);
        int          lastIndex = 0;
        int          escIndex  = message.length();

        // removed test: i < add.length
        // because if mainErrorMessage is equal to "blabla $$"
        // then the statement escIndex = mainErrorMessage.length();
        // is never reached!  ???
        for (int i = 0; i < add.length; i++) {
            escIndex = message.indexOf(MESSAGE_TAG, lastIndex);

            if (escIndex == -1) {
                break;
            }

            sb.append(message.substring(lastIndex, escIndex));
            sb.append(add[i] == null ? "null exception message"
                                     : add[i].toString());

            lastIndex = escIndex + MESSAGE_TAG.length();
        }

        escIndex = message.length();

        sb.append(message.substring(lastIndex, escIndex));

        return sb.toString();
    }

    /**
     * Returns the error message given the error code.<br/>
     * This method is be used when throwing exception other
     * than <code>HsqlException</code>.
     *
     * @param errorCode    the error code associated to the error message
     * @return  the error message associated with the error code
     */
    public static String getMessage(final int errorCode) {
        return getMessage(errorCode, 0, null);
    }

    /**
     * Returns the error SQL STATE sting given the error code.<br/>
     * This method is be used when throwing exception based on other exceptions.
     *
     * @param errorCode    the error code associated to the error message
     * @return  the error message associated with the error code
     */
    public static String getStateString(final int errorCode) {
        return getMessage(errorCode, 0, null).substring(0, SQL_STATE_DIGITS);
    }

    /**
     * Returns the error message given the error code.<br/> This method is used
     * when throwing exception other than <code>HsqlException</code>.
     *
     * @param code the code for the error message
     * @param subCode the code for the addon message
     * @param add value(s) to use to replace the placeholer(s)
     * @return the error message associated with the error code
     */
    public static String getMessage(final int code, int subCode,
                                    final Object[] add) {

        String message = getResourceString(code);

        if (subCode != 0) {
            message += getResourceString(subCode);
        }

        if (add != null) {
            message = insertStrings(message, add);
        }

        return message;
    }

    private static String getResourceString(int code) {

        String key = StringUtil.toZeroPaddedString(code, SQL_CODE_DIGITS,
            SQL_CODE_DIGITS);

        return BundleHandler.getString(bundleHandle, key);
    }

    public static HsqlException error(final Result result) {
        return new HsqlException(result);
    }

    /**
     * Used to print messages to System.out
     *
     *
     * @param message message to print
     */
    public static void printSystemOut(String message) {

        if (TRACESYSTEMOUT) {
            System.out.println(message);
        }
    }

    /**
     * Used to print messages to System.out
     *
     *
     * @param message1 message to print
     * @param message2 message to print
     */
    public static void printSystemOut(String message1, long message2) {

        if (TRACESYSTEMOUT) {
            System.out.print(message1);
            System.out.println(message2);
        }
    }
}
