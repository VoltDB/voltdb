/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltdb.VoltTable;
import org.voltdb.client.ProcCallException;

/**
 * <p>Runnable-implementor that fetches partition count using VoltDB system procedure "@GetPartitionKeys".
 * </p>
 *
 * <p>Every time that it's called, it updates a global data structure. This
 * structure is primarily used by the ContinuousDeleter class.</p>
 *
 * <p>This code is pretty adaptable to other applications without much
 * modification.</p>
 *
 */
public class PartitionDataTracker implements Runnable {

    // Global state
    final WindowingApp app;

    // track failures for reporting
    final AtomicLong failureCount = new AtomicLong(0);

    public PartitionDataTracker(WindowingApp app) {
        this.app = app;
    }

    @Override
    public void run() {
         try {
                VoltTable results[] = app.client.callProcedure("@GetPartitionKeys", "integer").getResults();
                app.updatePartitionInfo(results[0].getRowCount());
        } catch (IOException | ProcCallException e) {
            failureCount.incrementAndGet();
        }
     }
}
