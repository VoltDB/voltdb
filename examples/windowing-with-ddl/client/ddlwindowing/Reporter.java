/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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


package ddlwindowing;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import ddlwindowing.WindowingApp.PartitionInfo;

/**
 * Periodically print a report of insert rates, latencies and failures,
 * as well as average values of the 'val' column in the 'timedata' table
 * over several moving windows. Also print per-partition statistics collected
 * by PartitionDataTracker.  The delay between report printing is specified
 * by the user in the configuration.
 */
public class Reporter implements Runnable {

    final WindowingApp app;

    Reporter(WindowingApp app) {
        this.app = app;
    }

    @Override
    public void run() {
        boolean success = true;
        Map<Integer, Long> averagesForWindows = new TreeMap<Integer, Long>();
        // For several time windows, fetch the average value of 'val' for that
        // time window.
        // Note, this could be easily be combined into a stored proc and made
        // a single transactional call if one wanted to.
        // See ddl.sql for the actual SQL being run by the 'Average' procedure.
        for (int seconds : new int[] { 1, 5, 10, 30 }) {
            try {
                // SQL BEING RUN:
                //  SELECT SUM(sum_values) / SUM(count_values)
                //  FROM agg_by_second
                //  WHERE second_ts >= TO_TIMESTAMP(SECOND, SINCE_EPOCH(SECOND, NOW) - ?);
                ClientResponse cr = app.client.callProcedure("Average", seconds);
                VoltTable result = cr.getResults()[0];
                long average = result.asScalarLong();
                if (! result.wasNull()) {
                    averagesForWindows.put(seconds, average);
                } else {
                    // If there are no rows in the selected time window (for example
                    // if we stop the client and then start it up again), then the
                    // average will be NULL.
                    averagesForWindows.put(seconds, null);
                }
            } catch (IOException | ProcCallException e) {
                // Note any failure for reporting later.
                success = false;
            }
        }

        // Lock protects other (well-behaved) printing from being interspersed with
        // this report printing.
        synchronized(app) {
            long now = System.currentTimeMillis();
            long time = Math.round((now - app.startTS) / 1000.0);

            // Print out how long the processing has been running
            System.out.printf("%02d:%02d:%02d Report:\n", time / 3600, (time / 60) % 60, time % 60);

            // If possible, print out the averages over several time windows.
            if (success) {
                System.out.println("  Average values over time windows:");
                for (Entry<Integer, Long> e : averagesForWindows.entrySet()) {
                    System.out.printf("    Average for past %2ds: %d\n", e.getKey(), e.getValue());
                }
            }
            else {
                System.out.println("  Unable to retrieve average values at this time.");
            }

            System.out.println("  Partition statistics:");
            for (Entry<Long, PartitionInfo> e : app.getPartitionData().entrySet()) {
                PartitionInfo pinfo = e.getValue();
                System.out.printf("    Partition %2d: %9d tuples, youngest: %6.3fs, oldest: %6.3fs\n",
                        e.getKey(), pinfo.tupleCount,
                        pinfo.youngestTupleAge / 1000.0, pinfo.oldestTupleAge / 1000.0);
            }

            // Let the inserter process print a one line report.
            app.inserter.printReport();

            //
            // FAILURE REPORTING FOR PERIODIC OPERATIONS
            //
            long partitionTrackerFailures = app.partitionTracker.failureCount.getAndSet(0);
            if (partitionTrackerFailures > 0) {
                System.out.printf("  Partition Tracker failed %d times since last report.\n",
                                  partitionTrackerFailures);
            }
            long maxTrackerFailures = app.maxTracker.failureCount.getAndSet(0);
            if (maxTrackerFailures > 0) {
                System.out.printf("  Max Tracker failed %d times since last report.\n",
                                  maxTrackerFailures);
            }

            System.out.println();
            System.out.flush();
        }
    }
}
