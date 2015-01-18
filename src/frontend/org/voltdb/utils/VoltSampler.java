/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** A minimal pure Java sampling profiler. */
public class VoltSampler extends Thread {

    public static class SampleUnit implements Comparable<SampleUnit> {
        public String className;
        public String methodName;
        int inclusiveSamples = 0;
        int endpointSamples = 0;

        public static boolean compareEndpoints = false;

        @Override
        public int compareTo(SampleUnit other) {
            assert(other != null);
            if (compareEndpoints)
                return other.endpointSamples - endpointSamples;
            else
                return other.inclusiveSamples - inclusiveSamples;
        }
    }

    public static class PerThreadData {
        public String name;
        public Map<String, SampleUnit> samples = new HashMap<String, SampleUnit>();
        public int nativeSamples = 0;
        public int javaSamples = 0;
        public int parkedSamples = 0;
    }

    // used during collection
    private int intersampleMillis;
    private String outputPath;
    private final AtomicBoolean doStop = new AtomicBoolean(false);
    private final Deque<Map<Thread, StackTraceElement[]>> samples = new ArrayDeque<Map<Thread, StackTraceElement[]>>();

    // used during analysis and transformation
    private final TreeMap<String, PerThreadData> m_data = new TreeMap<String, PerThreadData>();

    public VoltSampler(int intersampleMillis, String outputPath) {
        this.intersampleMillis = intersampleMillis;
        this.outputPath = outputPath;
    }

    public void sample() {
        // Note: Thread.getAllStackTraces seems to be a bit faster than
        // ThreadMXBean.dumpAllThreads.
        // 187190 < 214187 ns / call
        // ThreadMXBean.getAllThreadIds() seems to be faster than
        // Thread.enumerate
        // 3662 < 7389 ns / call
        // Filtering Thread objects from Thread.enumerate() then using
        // Thread.getStackTrace() on
        // only the threads we care about is slower than getting all stack
        // traces, even if we only
        // call Thread.enumerate every 128th time. Same with using
        // ThreadMXBean.getAllThreadIds()
        // then getThreadInfo()
        samples.add(Thread.getAllStackTraces());
    }

    static final String[] SPECIAL_NAMES = { "Finalizer", "Reference Handler", "Dispatcher" };

    public void dumpSamples(PrintStream out, Thread sampleThread) {
        // reset from any previous dump
        m_data.clear();

        // The set of threads that will not be dumped
        HashSet<Thread> ignoreThreads = new HashSet<Thread>();

        // Ignore the thread that was sampling
        ignoreThreads.add(sampleThread);

        // Ignore the "special" threads
        for (Map.Entry<Thread, StackTraceElement[]> entry : Thread
                .getAllStackTraces().entrySet()) {
            Thread t = entry.getKey();
            for (String special : SPECIAL_NAMES) {
                if (t.getName().startsWith(special)) {
                    ignoreThreads.add(t);
                    break;
                }
            }

            if (entry.getValue().length == 0) {
                // Many JVM daemon threads have no stack
                ignoreThreads.add(t);
            }
        }

        // populate all the data structures
        for (Map<Thread, StackTraceElement[]> sample : samples) {
            for (Map.Entry<Thread, StackTraceElement[]> entry : sample.entrySet()) {
                if (ignoreThreads.contains(entry.getKey()))
                    continue;

                assert entry.getValue().length > 0;
                StackTraceElement[] elements = entry.getValue();
                for (int i = 0; i < elements.length; i++) {
                    incrementCounters(entry.getKey(), elements[i], i == 0);

                    //out.println(elements[i].toString());
                }
                //out.println();
            }
        }

        for (Entry<String, PerThreadData> e : m_data.entrySet()) {
            final int MAX_LINES = 50;
            PerThreadData ptd = e.getValue();
            int totalSamples = ptd.javaSamples + ptd.nativeSamples;

            out.printf("\n====================================================\n");
            out.printf("THREAD: %s\n", ptd.name);
            out.printf("====================================================\n\n");

            out.printf("%d native and %d java samples at %.2f%% java\n\n",
                    ptd.nativeSamples, ptd.javaSamples,
                    ptd.javaSamples * 100.0 / totalSamples);

            out.printf("%d/%d parked samples at %.2f%% parked\n\n",
                    ptd.parkedSamples, totalSamples,
                    ptd.parkedSamples * 100.0 / totalSamples);

            out.printf(" = All Methods by Inclusive Time = \n\n");

            ArrayList<SampleUnit> unitsList = new ArrayList<SampleUnit>();
            unitsList.addAll(ptd.samples.values());
            SampleUnit[] units = unitsList.toArray(new SampleUnit[0]);
            SampleUnit.compareEndpoints = false;
            Arrays.sort(units);
            //Collections.sort(units);
            int maxIndex = (units.length > MAX_LINES) ? MAX_LINES : units.length;
            for (int i = 0; i < maxIndex; i++) {
                SampleUnit unit = units[i];
                out.printf("%80s - %6d/%3.2f%% in method, %6d/%3.2f%% in method code.\n",
                        unit.className + "." + unit.methodName,
                        unit.inclusiveSamples, unit.inclusiveSamples * 100.0 / totalSamples,
                        unit.endpointSamples, unit.endpointSamples * 100.0 / totalSamples);
            }

            int i = 0;
            int found = 0;
            out.printf("\n = Restricted to VoltDB Methods by Inclusive Time = \n\n");
            while ((i < units.length) && (found < MAX_LINES)) {
                SampleUnit unit = units[i++];
                if (unit.className.startsWith("org.voltdb") == false)
                    continue;
                found++;
                out.printf("%80s - %6d/%3.2f%% in method, %6d/%3.2f%% in method code.\n",
                        unit.className + "." + unit.methodName,
                        unit.inclusiveSamples, unit.inclusiveSamples * 100.0 / totalSamples,
                        unit.endpointSamples, unit.endpointSamples * 100.0 / totalSamples);
            }

            out.printf("\n = All Methods by In-Method Time = \n\n");

            SampleUnit.compareEndpoints = true;
            Arrays.sort(units);
            //Collections.sort(units);
            for (i = 0; i < maxIndex; i++) {
                SampleUnit unit = units[i];
                out.printf("%80s - %6d/%3.2f%% in method, %6d/%3.2f%% in method code.\n",
                        unit.className + "." + unit.methodName,
                        unit.inclusiveSamples, unit.inclusiveSamples * 100.0 / totalSamples,
                        unit.endpointSamples, unit.endpointSamples * 100.0 / totalSamples);
            }

            i = 0;
            found = 0;
            out.printf("\n = Restricted to VoltDB Methods by In-Method Time = \n\n");
            while ((i < units.length) && (found < MAX_LINES)) {
                SampleUnit unit = units[i++];
                if (unit.className.startsWith("org.voltdb") == false)
                    continue;
                found++;
                out.printf("%80s - %6d/%3.2f%% in method, %6d/%3.2f%% in method code.\n",
                        unit.className + "." + unit.methodName,
                        unit.inclusiveSamples, unit.inclusiveSamples * 100.0 / totalSamples,
                        unit.endpointSamples, unit.endpointSamples * 100.0 / totalSamples);
            }
        }

    }

    void incrementCounters(final Thread thread, StackTraceElement element, boolean endpoint) {
        String fullName = element.getClassName() + "." + element.getMethodName();
        PerThreadData ptd = m_data.get(thread.getName());
        if (ptd == null) {
            ptd = new PerThreadData();
            ptd.name = thread.getName();
            m_data.put(thread.getName(), ptd);
        }
        SampleUnit unit = ptd.samples.get(fullName);
        if (unit == null) {
            unit = new SampleUnit();
            unit.className = element.getClassName();
            unit.methodName = element.getMethodName();
            ptd.samples.put(fullName, unit);
        }
        unit.inclusiveSamples++;
        if (endpoint) {
            unit.endpointSamples++;
            if (unit.methodName.startsWith("native")) ptd.nativeSamples++;
            else ptd.javaSamples++;
            if (unit.methodName.equals("park")) ptd.parkedSamples++;
        }
    }

    @Override
    public void run() {
        assert Thread.currentThread().getThreadGroup().getParent() == null;
        System.err.println("sampling every " + intersampleMillis);

        long start = System.currentTimeMillis();
        while (!doStop.get()) {
            sample();
            try {
                Thread.sleep(intersampleMillis);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        long end = System.currentTimeMillis();

        System.err.println("duration = " + (end - start) + " " + samples.size()
                + " samples; real rate = " + ((end - start) / samples.size()));
        try {
            PrintStream out = new PrintStream(new File(outputPath));
            dumpSamples(out, Thread.currentThread());
            out.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setShouldStop() {
        this.doStop.set(true);
    }
}
