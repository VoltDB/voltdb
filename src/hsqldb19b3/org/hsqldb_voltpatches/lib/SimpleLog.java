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
 * @version 1.9.0
 * @since 1.8.0
 */
public class SimpleLog {

    public static final int LOG_NONE   = 0;
    public static final int LOG_ERROR  = 1;
    public static final int LOG_NORMAL = 2;
    private PrintWriter     writer;
    private int             level;
    private boolean         isSystem;

    public SimpleLog(String path, int level, boolean useFile) {

        this.level = level;

        if (level != LOG_NONE) {
            if (useFile) {
                File file = new File(path);

                makeLog(file);
            } else {
                isSystem = true;
                writer = new PrintWriter(System.out);
            }
        }
    }

    private void makeLog(File file) {

        try {
            FileUtil.getDefaultInstance().makeParentDirectories(file);

            writer = new PrintWriter(new FileWriter(file.getPath(), true),
                                     true);
        } catch (Exception e) {
            isSystem = true;
            writer = new PrintWriter(System.out);
        }
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public PrintWriter getPrintWriter() {
        return writer;
    }

    public synchronized void sendLine(int atLevel, String message) {

        if (level >= atLevel) {
            writer.println(HsqlDateTime.getSytemTimeString() + " " + message);
        }
    }

    public synchronized void logContext(int atLevel, String message) {

        if (level < atLevel) {
            return;
        }

        String info = HsqlDateTime.getSytemTimeString();

//#ifdef JAVA4
        Throwable           temp     = new Throwable();
        StackTraceElement[] elements = temp.getStackTrace();

        if (elements.length > 1) {
            info += " " + elements[1].getClassName() + "."
                    + elements[1].getMethodName();
        }

//#endif JAVA4
        writer.println(info + " " + message);
    }

    public synchronized void logContext(Throwable t, String message) {

        if (level == LOG_NONE) {
            return;
        }

        String info = HsqlDateTime.getSytemTimeString();

//#ifdef JAVA4
        Throwable           temp     = new Throwable();
        StackTraceElement[] elements = temp.getStackTrace();

        if (elements.length > 1) {
            info += " " + elements[1].getClassName() + "."
                    + elements[1].getMethodName();
        }

        elements = t.getStackTrace();

        if (elements.length > 0) {
            info += " " + elements[0].getClassName() + "."
                    + elements[0].getMethodName();
        }

//#endif JAVA4
        if (message == null) {
            message = "";
        }

        writer.println(info + " " + t.toString() + " " + message);
    }

    public void flush() {

        if (writer != null) {
            writer.flush();
        }
    }

    public void close() {

        if (writer != null && !isSystem) {
            writer.close();
        }

        writer = null;
    }
}
