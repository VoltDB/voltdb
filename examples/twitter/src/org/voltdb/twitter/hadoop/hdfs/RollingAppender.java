/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
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
