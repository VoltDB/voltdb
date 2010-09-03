/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.twitter.hadoop.hdfs;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.voltdb.logging.VoltLogger;

public class RollingAppender {

    private static final VoltLogger LOG = new VoltLogger("RollingAppender");

    private FileSystem hdfs;
    private String uri;
    private String table;

    private PrintWriter out;
    private int index = 0;

    public RollingAppender(FileSystem hdfs, String uri, String table) {
        this.hdfs = hdfs;
        this.uri = uri;
        this.table = table;

        roll();

        // roll the output file every 60 seconds, starting at the next minute mark
        Calendar now = new GregorianCalendar();
        Calendar start = new GregorianCalendar(
                    now.get(Calendar.YEAR),
                    now.get(Calendar.MONTH),
                    now.get(Calendar.DAY_OF_MONTH),
                    now.get(Calendar.HOUR_OF_DAY),
                    now.get(Calendar.MINUTE) + 1);
        long delay = start.getTimeInMillis() - now.getTimeInMillis();
        new Timer(true).scheduleAtFixedRate(new TimerTask() {

            public void run() {
                roll();
            }

        }, delay, 1000L * 60L);
    }

    private synchronized void roll() {
        try {
            if (out != null) {
                out.close();
            }

            String pathString = String.format("%s/%s-%04d.txt", uri, table, index);
            Path path = new Path(pathString);
            hdfs.create(path).close();
            out = new PrintWriter(hdfs.append(path));
            index++;

            LOG.info("Rolled output file to: " + pathString);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void append(String line) {
        out.println(line);
    }

}
