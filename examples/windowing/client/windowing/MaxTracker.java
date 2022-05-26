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
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

/**
 * Runnable-implementer that fetches the maximum value in
 * the 'timedata' table when asked. Prints out a message to
 * the console when the maximum changes (or is initially set).
 *
 */
public class MaxTracker implements Runnable {

    // Global state
    final WindowingApp app;

    long previousMax = Long.MIN_VALUE;

    // track failures for reporting
    final AtomicLong failureCount = new AtomicLong(0);

    MaxTracker(WindowingApp app) {
        this.app = app;
    }

    @Override
    public void run() {
        try {
            // Call a proc (synchronously) to get the maximum value.
            // See ddl.sql for the actual SQL being run.
            // Note this is a cross-partition transaction, but as
            // of VoltDB 4.0, it should be fast as it's a read that
            // only needs to make one round trip to all partitions.
            //
            // SQL BEING RUN:
            //  SELECT MAX(val) FROM timedata;
            ClientResponse cr = app.client.callProcedure("MaxValue");
            long currentMax = cr.getResults()[0].asScalarLong();

            if (currentMax == previousMax) {
                return;
            }

            // Output synchronized on global state to make this line not print in the middle
            // of other reporting lines.
            synchronized(app) {
                if (previousMax == Long.MIN_VALUE) {
                    System.out.printf("The initial maximum value for the dataset has been set to %d.\n\n",
                                      currentMax);
                }
                else {
                    System.out.printf("The maximum value for the dataset has changed from %d to %d.\n\n",
                                      previousMax, currentMax);
                }
            }

            previousMax = currentMax;
        }
        catch (IOException | ProcCallException e) {
            // track failures in a pretty simple way for the reporter task
            failureCount.incrementAndGet();
        }
    }
}
