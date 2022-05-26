/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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


package windowing;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

/**
 * Periodically print a report of insert rates, latencies and failures,
 * as well as average values of the 'val' column in the 'timedata' table
 * over several moving windows. The delay between report printing is
 * specified by the user in the configuration.
 *
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
                //  WHERE second_ts >= DATEADD(SECOND, CAST(? as INTEGER), NOW);
                ClientResponse cr = app.client.callProcedure("Average", -seconds);
                long average = cr.getResults()[0].asScalarLong();
                averagesForWindows.put(seconds, average);
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

            // Let the inserter process print a one line report.
            app.inserter.printReport();

            // Print out an update on how many tuples have been deleted.
            System.out.printf("  Deleted %d tuples since last report\n", app.getDeletesSinceLastChecked());

            //
            // FAILURE REPORTING FOR PERIODIC OPERATIONS
            //
            long continuousDeleterFailures = app.deleter.failureCount.getAndSet(0);
            if (continuousDeleterFailures > 0) {
                System.out.printf("  Continuous Deleter failed %d times since last report.\n",
                                  continuousDeleterFailures);
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
