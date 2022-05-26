/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram_voltpatches.AbstractHistogram;
import org.HdrHistogram_voltpatches.Histogram;
import org.voltcore.utils.CompressionStrategySnappy;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;

public class LatencyLogger {
    private static Histogram m_histogramData;

    public static String usage() {
        return "Usage1: server port reportIntervalSeconds [hostname]\n" +
               "Usage2: filename";
    }

    public static void main(String[] args) throws Exception {
        final String hostname;
        if (args.length == 1) {
            readHistogramFromFile(args[0]);
            return;
        }
        if (args.length != 3) {
            if (args.length == 4) {
                hostname = args[3];
            } else {
                System.out.println(usage());
                return;
            }
        } else {
            hostname = args[0];
        }
        final String server = args[0];
        int dur = 0;
        try {
            dur = Integer.valueOf(args[2]);
            if (dur < 1) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            System.out.println(
                    "reportIntervalSeconds should be greater than or equal to 1");
            System.out.println(usage());
            System.exit(0);
        }
        final int duration = dur;
        // start with an empty password
        String username = "";
        String password = "";

        // if credentials set in env, use them
        if (System.getenv().containsKey("VOLTDBUSER")) {
            username = System.getenv("VOLTDBUSER");
        }
        if (System.getenv().containsKey("VOLTDBPASSWORD")) {
            password = System.getenv("VOLTDBPASSWORD");
        }

        // create the client with our credentials
        ClientConfig clientConfig = new ClientConfig(username, password);
        final Client c = ClientFactory.createClient(clientConfig);

        int port = 0;
        try {
            port = Integer.valueOf(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Failed to parse port number.");
            System.out.println("Usage server port reportIntervalSeconds");
            System.exit(0);
        }
        System.out.println("Connecting to " + server + " port " + port);
        c.createConnection(args[0], port);

        System.out.printf(
                "%12s, %10s, %10s, %10s, %10s, %10s, %10s, %10s\n",
                "TIMESTAMP", "COUNT", "TPS", "95", "99", "99.9", "99.99",
                "99.999");

        final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");

        ScheduledExecutorService ses = Executors
                .newSingleThreadScheduledExecutor();
       ses.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                VoltTable table = null;
                try {
                    table = c.callProcedure("@Statistics",
                            "LATENCY_HISTOGRAM", 0).getResults()[0];
                } catch (IOException | ProcCallException e) {
                    System.out.println("Failed to get statistics:");
                    e.printStackTrace();
                    System.exit(0);
                }
                List<String> hostnames = new ArrayList<String>();
                String tableHostname = "";
                while (!hostname.equalsIgnoreCase(tableHostname)) {
                    if (!table.advanceRow()) {
                        System.out.println("Server host name " + server
                                + " not found. Valid host names are "
                                + hostnames.toString());
                        System.exit(0);
                    }
                    tableHostname = table.getString(2);
                    hostnames.add(tableHostname);
                }
                Date now = new Date(table.getLong(0));
                Histogram newHistogram = AbstractHistogram
                        .fromCompressedBytes(table.getVarbinary(4),
                                CompressionStrategySnappy.INSTANCE);
                Histogram diffHistogram;
                if (m_histogramData == null) {
                    diffHistogram = newHistogram;
                } else {
                    diffHistogram = Histogram.diff(newHistogram,
                            m_histogramData);
                }
                long totalCount = diffHistogram.getTotalCount();
                if (totalCount > 0) {
                    System.out.printf(
                            "%12s, %10d, %10.0f, %8.2fms, %8.2fms, %8.2fms, %8.2fms, %8.2fms\n",
                            sdf.format(now), totalCount,
                            ((double) totalCount / duration),
                            (diffHistogram.getValueAtPercentile(95.0D)
                                    / 1000.0D),
                            (diffHistogram.getValueAtPercentile(99)
                                    / 1000.0D),
                            (diffHistogram.getValueAtPercentile(99.9)
                                    / 1000.0D),
                            (diffHistogram.getValueAtPercentile(99.99)
                                    / 1000.0D),
                            (diffHistogram.getValueAtPercentile(99.999)
                                    / 1000.0D));
                } else {
                    System.out.printf(
                            "%12s, %10d, %10d, %8.2fms, %8.2fms, %8.2fms, %8.2fms, %8.2fms\n",
                            sdf.format(now), totalCount, 0, 0D, 0D, 0D, 0D,
                            0D);
                }

                m_histogramData = AbstractHistogram.fromCompressedBytes(
                        table.getVarbinary(4),
                        CompressionStrategySnappy.INSTANCE);
                ;
            }
        }, 0, duration, TimeUnit.SECONDS);

    }

    public static void readHistogramFromFile(String filename) {
        System.out.println("Reading histograms from " + filename);
        Hashtable<String, Histogram> histograms = new Hashtable<String, Histogram>();
        Hashtable<String, Long> timestamps = new Hashtable<String, Long>();

        System.out.printf(
                "%23s, %32s, %10s, %10s, %10s, %10s, %10s, %10s, %10s\n",
                "TIMESTAMP", "HOST NAME", "COUNT", "TPS", "95", "99", "99.9", "99.99",
                "99.999");

        try {
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(new FileInputStream(filename)));

            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                StringTokenizer st = new StringTokenizer(line, ",");
                String time = st.nextToken().replaceAll("^\"|\"$", "");
                String host_id = st.nextToken().replaceAll("^\"|\"$", "");
                String host_name = st.nextToken().replaceAll("^\"|\"$", "");
                String site_id = st.nextToken().replaceAll("^\"|\"$", "");
                String histogram_val = st.nextToken().replaceAll("^\"|\"$", "");

                SimpleDateFormat sdf = new SimpleDateFormat(
                        "yyyy-MM-dd HH:mm:ss.SSS");
                String friendly_ts = sdf.format(new Date(Long.parseLong(time)));
                byte[] raw_histogram = Encoder.hexDecode(histogram_val);
                Histogram newHistogram = AbstractHistogram.fromCompressedBytes(
                        raw_histogram, CompressionStrategySnappy.INSTANCE);

                // Look for a prior histogram for this host
                Histogram priorHistogram = histograms.get(host_name);
                Long priorTime = timestamps.get(host_name);
                Histogram diffHistogram;
                long diffTime;
                if (priorHistogram == null) {
                    diffHistogram = newHistogram;
                    diffTime = 1;
                } else {
                    diffHistogram = Histogram.diff(newHistogram,
                            priorHistogram);

                    diffTime = (Long.parseLong(time) - priorTime)/1000;
                }
                // Set the most recent histogram as the prior histogram for this hostname.
                histograms.put(host_name, newHistogram);
                timestamps.put(host_name, Long.parseLong(time));

                long totalCount = diffHistogram.getTotalCount();
                if (totalCount > 0) {
                    System.out.printf(
                            "%23s, %32s, %10d, %10.0f, %8.2fms, %8.2fms, %8.2fms, %8.2fms, %8.2fms\n",
                            friendly_ts, host_name, totalCount,  (double) totalCount / diffTime,
                            (diffHistogram.getValueAtPercentile(95.0D)
                                    / 1000.0D),
                            (diffHistogram.getValueAtPercentile(99)
                                    / 1000.0D),
                            (diffHistogram.getValueAtPercentile(99.9)
                                    / 1000.0D),
                            (diffHistogram.getValueAtPercentile(99.99)
                                    / 1000.0D),
                            (diffHistogram.getValueAtPercentile(99.999)
                                    / 1000.0D));
                } else {
                    System.out.printf(
                            "%23s, %32s, %10d, %10d, %8.2fms, %8.2fms, %8.2fms, %8.2fms, %8.2fms\n",
                            friendly_ts, host_name, totalCount, 0, 0D, 0D, 0D,
                            0D, 0D);
                }
            }
            bufferedReader.close();

        } catch (FileNotFoundException e) {
            System.err.println(
                    "Histogram file '" + filename + "' could not be found.");
            System.exit(-1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        }
        //System.out.println("Number of Histograms: " + histograms.size());

    }

}
