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


package org.hsqldb_voltpatches.lib;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

import org.hsqldb_voltpatches.HsqlDateTime;

/**
 * Simple log for recording abnormal events in persistence<p>
 * Log levels, LOG_NONE, LOG_ERROR, and LOG_NORMAL are currently supported.<p>
 * LOG_ERROR corresponds to property value 1 and logs main database events plus
 * any major errors encountered in operation.
 * LOG_NORMAL corresponds to property value 2 and logs additional normal events
 * and minor errors.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.8.0
 */
public class SimpleLog {

    public static final int LOG_NONE   = 0;
    public static final int LOG_ERROR  = 1;
    public static final int LOG_NORMAL = 2;
    public static final int LOG_DETAIL = 3;
    public static final int LOG_RESULT = 4;

    //
    public static final String   logTypeNameEngine = "ENGINE";
    public static final String[] appLogTypeNames   = {
        "", "ERROR ", "NORMAL", "DETAIL"
    };
    public static final String[] sqlLogTypeNames   = {
        "", "BASIC ", "NORMAL", "DETAIL", "RESULT"
    };

    //
    private PrintWriter  writer;
    private int          level;
    private boolean      isSystem;
    private boolean      isSQL;
    String[]             logTypeNames;
    private String       filePath;
    private StringBuffer sb;

    public SimpleLog(String path, int level, boolean isSQL) {

        this.isSystem = path == null;
        this.filePath = path;
        this.isSQL    = isSQL;
        logTypeNames  = isSQL ? sqlLogTypeNames
                              : appLogTypeNames;
        sb            = new StringBuffer(256);

        setLevel(level);
    }

    private void setupWriter() {

        if (level == LOG_NONE) {
            close();

            return;
        }

        if (writer == null) {
            if (isSystem) {
                writer = new PrintWriter(System.out);
            } else {
                File file = new File(filePath);

                setupLog(file);
            }
        }
    }

    private void setupLog(File file) {

        try {
            FileUtil.getFileUtil().makeParentDirectories(file);

            writer = new PrintWriter(new FileWriter(file, true), true);
        } catch (Exception e) {
            isSystem = true;
            writer   = new PrintWriter(System.out);
        }
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {

        this.level = level;

        setupWriter();
    }

    public PrintWriter getPrintWriter() {
        return writer;
    }

    public synchronized void logContext(int atLevel, String message) {

        if (level < atLevel) {
            return;
        }

        if (writer == null) {
            return;
        }

        sb.append(HsqlDateTime.getSystemTimeString()).append(' ');

        if (!isSQL) {
            sb.append(logTypeNames[atLevel]).append(' ');
        }

        sb.append(message);
        writer.println(sb.toString());
        sb.setLength(0);
        writer.flush();
    }

    public synchronized void logContext(int atLevel, String prefix,
                                        String message, String suffix) {

        if (level < atLevel) {
            return;
        }

        if (writer == null) {
            return;
        }

        sb.append(HsqlDateTime.getSystemTimeString()).append(' ');

        if (!isSQL) {
            sb.append(logTypeNames[atLevel]).append(' ');
        }

        sb.append(prefix).append(' ');
        sb.append(message).append(' ').append(suffix);
        writer.println(sb.toString());
        sb.setLength(0);
        writer.flush();
    }

    public synchronized void logContext(Throwable t, String message,
                                        int atLevel) {

        if (level == LOG_NONE) {
            return;
        }

        if (writer == null) {
            return;
        }

        sb.append(HsqlDateTime.getSystemTimeString()).append(' ');

        if (!isSQL) {
            sb.append(logTypeNames[atLevel]).append(' ');
        }

        sb.append(message);

//#ifdef JAVA4
        Throwable           temp     = new Throwable();
        StackTraceElement[] elements = temp.getStackTrace();

        if (elements.length > 1) {
            sb.append(' ');
            sb.append(elements[1].getClassName()).append('.');
            sb.append(elements[1].getMethodName());
        }

        elements = t.getStackTrace();

        if (elements.length > 0) {
            sb.append(' ');
            sb.append(elements[0].getClassName()).append('.');
            sb.append(' ').append(elements[0].getMethodName());
        }

//#endif JAVA4
        sb.append(' ').append(t.toString());
        writer.println(sb.toString());
        sb.setLength(0);
        writer.flush();
    }

    public void flush() {

        if (writer != null) {
            writer.flush();
        }
    }

    public void close() {

        if (writer != null && !isSystem) {
            writer.flush();
            writer.close();
        }

        writer = null;
    }
}
