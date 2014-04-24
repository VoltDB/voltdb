/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

public class Reporter implements Runnable {

    final GlobalState state;
    RandomDataInserter inserter;

    Reporter(GlobalState state, RandomDataInserter inserter) {
        this.state = state;
        this.inserter = inserter;
    }

    @Override
    public void run() {
        Map<Integer, Long> averagesForWindows = new TreeMap<Integer, Long>();
        for (int seconds : new int[] { 1, 5, 10, 30 }) {
            try {
                ClientResponse cr = state.client.callProcedure("Average", seconds);
                long average = cr.getResults()[0].asScalarLong();
                averagesForWindows.put(seconds, average);
            } catch (IOException | ProcCallException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        synchronized(state) {
            long now = System.currentTimeMillis();
            long time = Math.round((now - state.benchmarkStartTS) / 1000.0);

            System.out.printf("%02d:%02d:%02d Report:\n", time / 3600, (time / 60) % 60, time % 60);

            System.out.println("  Average values over time windows:");
            for (Entry<Integer, Long> e : averagesForWindows.entrySet()) {
                System.out.printf("    Average for past %2ds: %d\n", e.getKey(), e.getValue());
            }

            inserter.printReport();

            System.out.println();
            System.out.flush();
        }
    }
}
