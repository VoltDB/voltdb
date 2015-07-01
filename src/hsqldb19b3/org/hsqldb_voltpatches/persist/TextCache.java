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


package org.hsqldb_voltpatches.persist;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.lib.FileUtil;
import org.hsqldb_voltpatches.lib.HsqlByteArrayOutputStream;
import org.hsqldb_voltpatches.rowio.RowInputInterface;
import org.hsqldb_voltpatches.rowio.RowInputText;
import org.hsqldb_voltpatches.rowio.RowInputTextQuoted;
import org.hsqldb_voltpatches.rowio.RowOutputText;
import org.hsqldb_voltpatches.rowio.RowOutputTextQuoted;
import org.hsqldb_voltpatches.scriptio.ScriptWriterText;
import org.hsqldb_voltpatches.store.ObjectCacheHashMap;

// Ito Kazumitsu 20030328 - patch 1.7.2 - character encoding support
// Dimitri Maziuk - patch for NL in string support
// sqlbob@users - updated for 1.8.0 to allow new-lines in fields
// fredt@users - updated for 1.8.0 to allow correct behaviour with transactions

/**
 * Acts as a buffer manager for a single TEXT table with respect its Row data.<p>
 *
 * Handles read/write operations on the table's text format data file using a
 * compatible pair of org.hsqldb_voltpatches.rowio input/output class instances.
 *
 *
 * @author Bob Preston (sqlbob@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.0
 */
public class TextCache extends DataFileCache {

    //state of Cache
    public static final String NL = System.getProperty("line.separator");
    public String              fs;
    public String              vs;
    public String              lvs;
    public String              stringEncoding;
    public boolean             isQuoted;
    public boolean             isAllQuoted;
    public boolean             ignoreFirst;
    protected String           header;
    protected Table            table;
    private ObjectCacheHashMap uncommittedCache;

    //
    final static char DOUBLE_QUOTE_CHAR = '\"';
    final static char BACKSLASH_CHAR    = '\\';
    final static char LF_CHAR           = '\n';
    final static char CR_CHAR           = '\r';

    /**
     *  The source string for a cached table is evaluated and the parameters
     *  are used to open the source file.<p>
     *
     *  Settings are used in this order: (1) settings specified in the
     *  source string for the table (2) global database settings in
     *  *.properties file (3) program defaults
     *
     *  fredt - this used to write rows as soon as they are inserted
     *  but now this is subject to session autoCommit / or commit
     *  storeOnInsert = true;
     */
    TextCache(Table table, String name) {

        super(table.database, name);

        this.table       = table;
        uncommittedCache = new ObjectCacheHashMap(5);
    }

    protected void initParams(Database database, String baseFileName) {

        fileName      = baseFileName;
        this.database = database;
        fa            = FileUtil.getDefaultInstance();

        HsqlProperties tableprops =
            HsqlProperties.delimitedArgPairsToProps(fileName, "=", ";", null);

        // source file name is the only key without a value
        fileName = tableprops.errorKeys[0].trim();

        //-- Get separators:
        HsqlDatabaseProperties dbProps = database.getProperties();

        fs = translateSep(tableprops.getProperty("fs",
                dbProps.getProperty(HsqlDatabaseProperties.textdb_fs, ",")));
        vs = translateSep(tableprops.getProperty("vs",
                dbProps.getProperty(HsqlDatabaseProperties.textdb_vs, fs)));
        lvs = translateSep(tableprops.getProperty("lvs",
                dbProps.getProperty(HsqlDatabaseProperties.textdb_lvs, fs)));

        //-- Get booleans
        ignoreFirst = tableprops.isPropertyTrue(
            "ignore_first",
            dbProps.isPropertyTrue(
                HsqlDatabaseProperties.textdb_ignore_first, false));
        isQuoted = tableprops.isPropertyTrue(
            "quoted",
            dbProps.isPropertyTrue(
                HsqlDatabaseProperties.textdb_quoted, true));
        isAllQuoted = tableprops.isPropertyTrue(
            "all_quoted",
            dbProps.isPropertyTrue(
                HsqlDatabaseProperties.textdb_all_quoted, false));

        //-- Get encoding
        stringEncoding = translateSep(tableprops.getProperty("encoding",
                dbProps.getProperty(HsqlDatabaseProperties.textdb_encoding,
                                    "ASCII")));

        //-- Get size and scale
        int cacheScale = tableprops.getIntegerProperty(
            "cache_scale",
            dbProps.getIntegerProperty(
                HsqlDatabaseProperties.textdb_cache_scale, 10, 8, 16));
        int cacheSizeScale = tableprops.getIntegerProperty(
            "cache_size_scale",
            dbProps.getIntegerProperty(
                HsqlDatabaseProperties.textdb_cache_size_scale, 10, 8, 20));
        int lookupTableLength = 1 << cacheScale;
        int avgRowBytes       = 1 << cacheSizeScale;

        maxCacheSize     = lookupTableLength * 3;
        maxCacheBytes    = maxCacheSize * avgRowBytes;
        maxDataFileSize  = Integer.MAX_VALUE;
        cachedRowPadding = 1;
        cacheFileScale   = 1;
    }

    static void checkTextSouceString(String fileName,
                                     HsqlDatabaseProperties dbProps) {

        HsqlProperties tableprops =
            HsqlProperties.delimitedArgPairsToProps(fileName, "=", ";", null);

        //-- Get file name
        switch (tableprops.errorCodes.length) {

            case 0 :
                throw Error.error(ErrorCode.X_S0501);
            case 1 :

                // source file name is the only key without a value
                fileName = tableprops.errorKeys[0].trim();
                break;

            default :
                throw Error.error(ErrorCode.X_S0502);
        }

        //-- Get separators:
        String fs = translateSep(tableprops.getProperty("fs",
            dbProps.getProperty(HsqlDatabaseProperties.textdb_fs, ",")));
        String vs = translateSep(tableprops.getProperty("vs",
            dbProps.getProperty(HsqlDatabaseProperties.textdb_vs, fs)));
        String lvs = translateSep(tableprops.getProperty("lvs",
            dbProps.getProperty(HsqlDatabaseProperties.textdb_lvs, fs)));

        if (fs.length() == 0 || vs.length() == 0 || lvs.length() == 0) {
            throw Error.error(ErrorCode.X_S0503);
        }
    }

    protected void initBuffers() {

        if (isQuoted || isAllQuoted) {
            rowIn = new RowInputTextQuoted(fs, vs, lvs, isAllQuoted);
            rowOut = new RowOutputTextQuoted(fs, vs, lvs, isAllQuoted,
                                             stringEncoding);
        } else {
            rowIn  = new RowInputText(fs, vs, lvs, false);
            rowOut = new RowOutputText(fs, vs, lvs, false, stringEncoding);
        }
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

    /**
     *  Opens a data source file.
     */
    public void open(boolean readonly) {

        fileFreePosition = 0;

        try {
            dataFile = ScaledRAFile.newScaledRAFile(database, fileName,
                    readonly, ScaledRAFile.DATA_FILE_RAF, null, null);
            fileFreePosition = dataFile.length();

            if (fileFreePosition > Integer.MAX_VALUE) {
                throw new HsqlException("", "", 0);
            }

            initBuffers();
        } catch (Exception e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_TextCache_openning_file_error,
                              new Object[] {
                fileName, e
            });
        }

        cacheReadonly = readonly;
    }

    void reopen() {
        open(cacheReadonly);
    }

    /**
     *  Writes newly created rows to disk. In the current implentation,
     *  such rows have already been saved, so this method just removes a
     *  source file that has no rows.
     */
    public synchronized void close(boolean write) {

        if (dataFile == null) {
            return;
        }

        try {
            cache.saveAll();

            boolean empty = (dataFile.length() <= NL.length());

            dataFile.close();

            dataFile = null;

            if (empty && !cacheReadonly) {
                FileUtil.getDefaultInstance().delete(fileName);
            }
        } catch (Exception e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_TextCache_closing_file_error,
                              new Object[] {
                fileName, e
            });
        }
    }

    /**
     * Closes the source file and deletes it if it is not read-only.
     */
    void purge() {

        uncommittedCache.clear();

        try {
            if (cacheReadonly) {
                close(false);
            } else {
                if (dataFile != null) {
                    dataFile.close();

                    dataFile = null;
                }

                FileUtil.getDefaultInstance().delete(fileName);
            }
        } catch (Exception e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_TextCache_purging_file_error,
                              new Object[] {
                fileName, e
            });
        }
    }

    /**
     *
     */
    public synchronized void remove(int pos, PersistentStore store) {

        CachedObject row = (CachedObject) uncommittedCache.remove(pos);

        if (row != null) {
            return;
        }

        row = cache.release(pos);

        clearRowImage(row);

//        release(pos);
    }

    public synchronized void removePersistence(int pos) {

        CachedObject row = (CachedObject) uncommittedCache.get(pos);

        if (row != null) {
            return;
        }

        row = cache.get(pos);

        clearRowImage(row);
    }

    private void clearRowImage(CachedObject row) {

        try {
            int length = row.getStorageSize()
                         - ScriptWriterText.BYTES_LINE_SEP.length;

            rowOut.reset();

            HsqlByteArrayOutputStream out = rowOut.getOutputStream();

            out.fill(' ', length);
            out.write(ScriptWriterText.BYTES_LINE_SEP);
            dataFile.seek(row.getPos());
            dataFile.write(out.getBuffer(), 0, out.size());
        } catch (IOException e) {
            throw Error.runtimeError(ErrorCode.U_S0500, e.getMessage());
        }
    }

    protected synchronized RowInputInterface readObject(int pos) {

        try {
            ByteArray buffer   = new ByteArray(80);
            boolean   complete = false;
            boolean   wasCR    = false;
            int       c;
            boolean   hasQuote  = false;
            boolean   wasNormal = false;

            pos = findNextUsedLinePos(pos);

            if (pos == -1) {
                return null;
            }

            dataFile.seek(pos);

            while (!complete) {
                wasNormal = false;
                c         = dataFile.read();

                if (c == -1) {
                    if (buffer.length() == 0) {
                        return null;
                    }

                    complete = true;

                    if (wasCR) {
                        break;
                    }

                    if (!cacheReadonly) {
                        dataFile.write(ScriptWriterText.BYTES_LINE_SEP, 0,
                                       ScriptWriterText.BYTES_LINE_SEP.length);
                    }

                    break;
                }

                switch (c) {

                    case DOUBLE_QUOTE_CHAR :
                        wasNormal = true;
                        complete  = wasCR;
                        wasCR     = false;

                        if (isQuoted) {
                            hasQuote = !hasQuote;
                        }
                        break;

                    case CR_CHAR :
                        wasCR = !hasQuote;
                        break;

                    case LF_CHAR :
                        complete = !hasQuote;
                        break;

                    default :
                        wasNormal = true;
                        complete  = wasCR;
                        wasCR     = false;
                }

                buffer.append(c);
            }

            if (complete) {
                int length = (int) dataFile.getFilePointer() - pos;

                if (wasNormal) {
                    length--;
                }

                ((RowInputText) rowIn).setSource(buffer.toString(), pos,
                                                 length);

                return rowIn;
            }

            return null;
        } catch (IOException e) {
            throw new HsqlException(e.getMessage(), "", 0);
        }
    }

    public int readHeaderLine() {

        boolean   complete  = false;
        boolean   wasCR     = false;
        boolean   wasNormal = false;
        ByteArray buffer    = new ByteArray(80);

        while (!complete) {
            wasNormal = false;

            int c;

            try {
                c = dataFile.read();

                if (c == -1) {
                    if (buffer.length() == 0) {
                        return 0;
                    }

                    complete = true;

                    if (!cacheReadonly) {
                        dataFile.write(ScriptWriterText.BYTES_LINE_SEP, 0,
                                       ScriptWriterText.BYTES_LINE_SEP.length);
                    }

                    break;
                }
            } catch (IOException e) {
                throw Error.error(ErrorCode.TEXT_FILE);
            }

            switch (c) {

                case CR_CHAR :
                    wasCR = true;
                    break;

                case LF_CHAR :
                    complete = true;
                    break;

                default :
                    wasNormal = true;
                    complete  = wasCR;
                    wasCR     = false;
            }

            buffer.append(c);
        }

        header = buffer.toString();

        try {
            int length = (int) dataFile.getFilePointer();

            if (wasNormal) {
                length--;
            }

            return length;
        } catch (IOException e) {
            throw Error.error(ErrorCode.TEXT_FILE);
        }
    }

    // fredt - new method

    /**
     * Searches from file pointer, pos, and finds the beginning of the first
     * line that contains any non-space character. Increments the row counter
     * when a blank line is skipped.
     *
     * If none found return -1
     */
    int findNextUsedLinePos(int pos) {

        try {
            int     firstPos   = pos;
            int     currentPos = pos;
            boolean wasCR      = false;

            dataFile.seek(pos);

            while (true) {
                int c = dataFile.read();

                currentPos++;

                switch (c) {

                    case CR_CHAR :
                        wasCR = true;
                        break;

                    case LF_CHAR :
                        wasCR = false;

                        ((RowInputText) rowIn).skippedLine();

                        firstPos = currentPos;
                        break;

                    case ' ' :
                        if (wasCR) {
                            wasCR = false;

                            ((RowInputText) rowIn).skippedLine();
                        }
                        break;

                    case -1 :
                        return -1;

                    default :
                        return firstPos;
                }
            }
        } catch (IOException e) {
            throw new HsqlException(e.getMessage(), "", 0);
        }
    }

    public synchronized void add(CachedObject object) {
        super.add(object);
        clearRowImage(object);
    }

    public synchronized CachedObject get(int i, PersistentStore store,
                                         boolean keep) {

        if (i < 0) {
            return null;
        }

        CachedObject o = (CachedObject) uncommittedCache.get(i);

        if (o == null) {
            o = super.get(i, store, keep);
        }

/*
        if (o == null) {
            o = super.get(i, store, keep);
        }
*/
        return o;
    }

    /**
     * This is called internally when old rows need to be removed from the
     * cache. Text table rows that have not been saved are those that have not
     * been committed yet. So we don't save them but add them to the
     * uncommitted cache until such time that they are committed or rolled
     * back- fredt
     */
    protected synchronized void saveRows(CachedObject[] rows, int offset,
                                         int count) {

        if (count == 0) {
            return;
        }

        for (int i = offset; i < offset + count; i++) {
            CachedObject r = rows[i];

            uncommittedCache.put(r.getPos(), r);

            rows[i] = null;
        }
    }

    /**
     * In case the row has been moved to the uncommittedCache, removes it.
     * Then saves the row as normal.
     */
    public synchronized void saveRow(CachedObject row) {
        uncommittedCache.remove(row.getPos());
        super.saveRow(row);
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {

        if (ignoreFirst && fileFreePosition == 0) {
            try {
                writeHeader(header);

                this.header = header;
            } catch (HsqlException e) {
                throw new HsqlException(
                    e, Error.getMessage(ErrorCode.GENERAL_IO_ERROR),
                    ErrorCode.GENERAL_IO_ERROR);
            }

            return;
        }

        throw Error.error(ErrorCode.TEXT_TABLE_HEADER);
    }

    private void writeHeader(String header) {

        try {
            byte[] buf       = null;
            String firstLine = header + NL;

            try {
                buf = firstLine.getBytes(stringEncoding);
            } catch (UnsupportedEncodingException e) {
                buf = firstLine.getBytes();
            }

            dataFile.write(buf, 0, buf.length);

            fileFreePosition = buf.length;
        } catch (IOException e) {
            throw new HsqlException(e.getMessage(), "", 0);
        }
    }

    private class ByteArray {

        private byte[] buffer;
        private int    buflen;

        public ByteArray(int n) {
            buffer = new byte[n];
            buflen = 0;
        }

        public void append(int c) {

            if (buflen >= buffer.length) {
                byte[] newbuf = new byte[buflen + 80];

                System.arraycopy(buffer, 0, newbuf, 0, buflen);

                buffer = newbuf;
            }

            buffer[buflen] = (byte) c;

            buflen++;
        }

        public int length() {
            return buflen;
        }

        public void setLength(int l) {
            buflen = l;
        }

        public String toString() {

            try {
                return new String(buffer, 0, buflen, stringEncoding);
            } catch (UnsupportedEncodingException e) {
                return new String(buffer, 0, buflen);
            }
        }
    }

    public int getLineNumber() {
        return ((RowInputText) rowIn).getLineNumber();
    }

    protected void setFileModified() {
        fileModified = true;
    }
}
