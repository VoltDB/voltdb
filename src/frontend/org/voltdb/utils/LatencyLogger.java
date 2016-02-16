/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram_voltpatches.AbstractHistogram;
import org.HdrHistogram_voltpatches.Histogram;
import org.voltcore.utils.CompressionStrategySnappy;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;

public class LatencyLogger {
    private static Histogram m_histogramData;

    public static String usage() {
        return "Usage server port reportIntervalSeconds [hostname]";
    }

    public static void main(String[] args) throws Exception {
        final String hostname;
        if (args.length != 3) {
            if (args.length == 4)
                hostname = args[3];
            else {
                System.out.println(usage());
                return;
            }
        } else
            hostname = args[0];

        final String server = args[0];
        int dur = 0;
        try {
            dur = Integer.valueOf(args[2]);
            if (dur < 1)
                throw new NumberFormatException();
        } catch (NumberFormatException e) {
            System.out.println("reportIntervalSeconds should be greater than or equal to 1");
            System.out.println(usage());
            System.exit(0);
        }
        final int duration = dur;

        final Client c = ClientFactory.createClient();
        int port = 0;
        try {
            port = Integer.valueOf(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Failed to parse port number.");
            System.out.println("Usage server port reportIntervalSeconds");
            System.exit(0);
        }
        System.out.println("Connecting to " + server + " port " + port);
        c.createConnection( args[0], port);

        System.out.printf("%12s, %10s, %10s, %10s, %10s, %10s, %10s, %10s\n", "TIMESTAMP", "COUNT", "TPS", "95", "99", "99.9", "99.99", "99.999");

        final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss:SSS");

        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
        ses.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    VoltTable table = null;
                    try {
                        table = c.callProcedure("@Statistics", "LATENCY_HISTOGRAM", 0).getResults()[0];
                    } catch (IOException|ProcCallException e) {
                        System.out.println("Failed to get statistics:");
                        e.printStackTrace();
                        System.exit(0);
                    }
                    List<String> hostnames = new ArrayList<String>();
                    String tableHostname = "";
                    while (!hostname.equalsIgnoreCase(tableHostname)) {
                        if (!table.advanceRow()) {
                            System.out.println("Server host name " + server + " not found. Valid host names are " + hostnames.toString());
                            System.exit(0);
                        }
                        tableHostname = table.getString(2);
                        hostnames.add(tableHostname);
                    }
                    Date now = new Date(table.getLong(0));
                    Histogram newHistogram = AbstractHistogram.fromCompressedBytes(table.getVarbinary(4), CompressionStrategySnappy.INSTANCE);
                    Histogram diffHistogram;
                    if (m_histogramData == null)
                         diffHistogram = newHistogram;
                     else
                         diffHistogram = Histogram.diff(newHistogram, m_histogramData);

                    long totalCount = diffHistogram.getTotalCount();
                    if (totalCount > 0)
                        System.out.printf("%12s, %10d, %10d, %8.2fms, %8.2fms, %8.2fms, %8.2fms, %8.2fms\n",
                                          sdf.format(now),
                                          totalCount,
                                          (totalCount / duration),
                                          (diffHistogram.getValueAtPercentile(95.0D) / 1000.0D),
                                          (diffHistogram.getValueAtPercentile(99) / 1000.0D),
                                          (diffHistogram.getValueAtPercentile(99.9) / 1000.0D),
                                          (diffHistogram.getValueAtPercentile(99.99) / 1000.0D),
                                          (diffHistogram.getValueAtPercentile(99.999) / 1000.0D));
                    else
                        System.out.printf("%12s, %10d, %10d, %8.2fms, %8.2fms, %8.2fms, %8.2fms, %8.2fms\n",
                                          sdf.format(now),
                                          totalCount,
                                          0,
                                          0D, 0D, 0D, 0D, 0D);

                    m_histogramData = AbstractHistogram.fromCompressedBytes(table.getVarbinary(4), CompressionStrategySnappy.INSTANCE);;
                }
            }, 0, duration, TimeUnit.SECONDS);
    }
}
