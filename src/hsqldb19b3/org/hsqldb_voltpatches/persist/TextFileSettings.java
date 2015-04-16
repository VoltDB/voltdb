/* Copyright (c) 2001-2011, The HSQL Development Group
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


package org.hsqldb_voltpatches.persist;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;

/**
 * Parser and container for text table settings.
 *
 * @author Bob Preston (sqlbob@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 2.2.6
 */
public class TextFileSettings {

    //state of Cache
    public static final String NL = System.getProperty("line.separator");
    public String              fs;
    public String              vs;
    public String              lvs;
    public String              stringEncoding;
    public boolean             isQuoted;
    public boolean             isAllQuoted;
    public boolean             ignoreFirst;

    //
    Database database;
    String   dataFileName;
    int      maxCacheRows;
    int      maxCacheBytes;

    //
    static final byte[] BYTES_LINE_SEP    = NL.getBytes();
    static final char   DOUBLE_QUOTE_CHAR = '\"';
    static final char   BACKSLASH_CHAR    = '\\';
    static final char   LF_CHAR           = '\n';
    static final char   CR_CHAR           = '\r';

    /**
     *  The source string for a cached table is evaluated and the parameters
     *  are used to open the source file.<p>
     *
     *  Settings are used in this order: (1) settings specified in the
     *  source string for the table (2) global database settings
     *  (3) program defaults
     */
    TextFileSettings(Database database, String fileSettingsString) {

        this.database = database;

        HsqlProperties tableprops =
            HsqlProperties.delimitedArgPairsToProps(fileSettingsString, "=",
                ";", "textdb");
        HsqlDatabaseProperties dbProps = database.getProperties();

        switch (tableprops.errorCodes.length) {

            case 0 :
                // no source file name
                this.dataFileName = null;
                break;

            case 1 :

                // source file name is the only key without a value
                this.dataFileName = tableprops.errorKeys[0].trim();
                break;

            default :
                throw Error.error(ErrorCode.X_S0502);
        }

        //-- Get separators: from database properties, then from table properties
        fs  = dbProps.getStringProperty(HsqlDatabaseProperties.textdb_fs);
        fs  = tableprops.getProperty(HsqlDatabaseProperties.textdb_fs, fs);
        vs  = dbProps.getStringProperty(HsqlDatabaseProperties.textdb_vs);
        vs  = tableprops.getProperty(HsqlDatabaseProperties.textdb_vs, vs);
        lvs = dbProps.getStringProperty(HsqlDatabaseProperties.textdb_lvs);
        lvs = tableprops.getProperty(HsqlDatabaseProperties.textdb_lvs, lvs);

        if (vs == null) {
            vs = fs;
        }

        if (lvs == null) {
            lvs = fs;
        }

        fs  = translateSep(fs);
        vs  = translateSep(vs);
        lvs = translateSep(lvs);

        if (fs.length() == 0 || vs.length() == 0 || lvs.length() == 0) {
            throw Error.error(ErrorCode.X_S0503);
        }

        //-- Get booleans
        ignoreFirst =
            dbProps.isPropertyTrue(HsqlDatabaseProperties.textdb_ignore_first);
        ignoreFirst = tableprops.isPropertyTrue(
            HsqlDatabaseProperties.textdb_ignore_first, ignoreFirst);
        isQuoted =
            dbProps.isPropertyTrue(HsqlDatabaseProperties.textdb_quoted);
        isQuoted =
            tableprops.isPropertyTrue(HsqlDatabaseProperties.textdb_quoted,
                                      isQuoted);
        isAllQuoted =
            dbProps.isPropertyTrue(HsqlDatabaseProperties.textdb_all_quoted);
        isAllQuoted =
            tableprops.isPropertyTrue(HsqlDatabaseProperties.textdb_all_quoted,
                                      isAllQuoted);
        stringEncoding =
            dbProps.getStringProperty(HsqlDatabaseProperties.textdb_encoding);
        stringEncoding =
            tableprops.getProperty(HsqlDatabaseProperties.textdb_encoding,
                                   stringEncoding);

        //-- get size and scale
        int cacheScale = dbProps.getIntegerProperty(
            HsqlDatabaseProperties.textdb_cache_scale);

        cacheScale = tableprops.getIntegerProperty(
            HsqlDatabaseProperties.textdb_cache_scale, cacheScale);

        int cacheSizeScale = dbProps.getIntegerProperty(
            HsqlDatabaseProperties.textdb_cache_size_scale);

        cacheSizeScale = tableprops.getIntegerProperty(
            HsqlDatabaseProperties.textdb_cache_size_scale, cacheSizeScale);

//
        maxCacheRows = (1 << cacheScale) * 3;
        maxCacheRows = dbProps.getIntegerProperty(
            HsqlDatabaseProperties.textdb_cache_rows, maxCacheRows);
        maxCacheRows = tableprops.getIntegerProperty(
            HsqlDatabaseProperties.textdb_cache_rows, maxCacheRows);
        maxCacheBytes = ((1 << cacheSizeScale) * maxCacheRows) / 1024;

        if (maxCacheBytes < 4) {
            maxCacheBytes = 4;
        }

        maxCacheBytes = dbProps.getIntegerProperty(
            HsqlDatabaseProperties.textdb_cache_size, (int) maxCacheBytes);
        maxCacheBytes = tableprops.getIntegerProperty(
            HsqlDatabaseProperties.textdb_cache_size, (int) maxCacheBytes);
        maxCacheBytes *= 1024;
    }

    String getFileName() {
        return dataFileName;
    }

    int getMaxCacheRows() {
        return maxCacheRows;
    }

    int getMaxCacheBytes() {
        return maxCacheBytes;
    }

    private static String translateSep(String sep) {
        return translateSep(sep, false);
    }

    /**
     * Translates the escaped characters in a separator string and returns
     * the non-escaped string.
     */
    private static String translateSep(String sep, boolean isProperty) {

        if (sep == null) {
            return null;
        }

        int next = sep.indexOf(BACKSLASH_CHAR);

        if (next != -1) {
            int          start    = 0;
            char[]       sepArray = sep.toCharArray();
            char         ch       = 0;
            int          len      = sep.length();
            StringBuffer sb       = new StringBuffer(len);

            do {
                sb.append(sepArray, start, next - start);

                start = ++next;

                if (next >= len) {
                    sb.append(BACKSLASH_CHAR);

                    break;
                }

                if (!isProperty) {
                    ch = sepArray[next];
                }

                if (ch == 'n') {
                    sb.append(LF_CHAR);

                    start++;
                } else if (ch == 'r') {
                    sb.append(CR_CHAR);

                    start++;
                } else if (ch == 't') {
                    sb.append('\t');

                    start++;
                } else if (ch == BACKSLASH_CHAR) {
                    sb.append(BACKSLASH_CHAR);

                    start++;
                } else if (ch == 'u') {
                    start++;

                    sb.append(
                        (char) Integer.parseInt(
                            sep.substring(start, start + 4), 16));

                    start += 4;
                } else if (sep.startsWith("semi", next)) {
                    sb.append(';');

                    start += 4;
                } else if (sep.startsWith("space", next)) {
                    sb.append(' ');

                    start += 5;
                } else if (sep.startsWith("quote", next)) {
                    sb.append(DOUBLE_QUOTE_CHAR);

                    start += 5;
                } else if (sep.startsWith("apos", next)) {
                    sb.append('\'');

                    start += 4;
                } else {
                    sb.append(BACKSLASH_CHAR);
                    sb.append(sepArray[next]);

                    start++;
                }
            } while ((next = sep.indexOf(BACKSLASH_CHAR, start)) != -1);

            sb.append(sepArray, start, len - start);

            sep = sb.toString();
        }

        return sep;
    }
}
