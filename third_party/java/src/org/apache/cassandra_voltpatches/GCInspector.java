/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra_voltpatches;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.voltcore.logging.VoltLogger;

import com.google_voltpatches.common.collect.ImmutableSet;

public class GCInspector
{
    private static final VoltLogger logger = new VoltLogger("GC");
    final static long INTERVAL_IN_MS = 1000;
    final static long MIN_DURATION = 200;

    public static final GCInspector instance = new GCInspector();

    private final HashMap<String, Long> gctimes = new HashMap<String, Long>();
    private final HashMap<String, Long> gccounts = new HashMap<String, Long>();

    final List<GarbageCollectorMXBean> beans = new ArrayList<GarbageCollectorMXBean>();
    final MemoryMXBean membean = ManagementFactory.getMemoryMXBean();

    public GCInspector()
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try
        {
            ObjectName gcName = new ObjectName(ManagementFactory.GARBAGE_COLLECTOR_MXBEAN_DOMAIN_TYPE + ",*");
            for (ObjectName name : server.queryNames(gcName, null))
            {
                GarbageCollectorMXBean gc = ManagementFactory.newPlatformMXBeanProxy(server, name.getCanonicalName(), GarbageCollectorMXBean.class);
                beans.add(gc);
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void start(ScheduledThreadPoolExecutor stpe)
    {
        // don't bother starting a thread that will do nothing.
        if (beans.size() == 0)
            return;
        Runnable t = new Runnable()
        {
            @Override
            public void run()
            {
                logGCResults();
            }
        };
        stpe.scheduleAtFixedRate(t, INTERVAL_IN_MS, INTERVAL_IN_MS, TimeUnit.MILLISECONDS);
    }

    private static final ImmutableSet<String> oldGenGCs =
            ImmutableSet.of(
                    "ConcurrentMarkSweep",
                    "PS MarkSweep",
                    "G1 Old Generation");

    private void logGCResults()
    {
        for (GarbageCollectorMXBean gc : beans)
        {
            Long previousTotal = gctimes.get(gc.getName());
            Long total = gc.getCollectionTime();
            if (previousTotal == null)
                previousTotal = 0L;
            if (previousTotal.equals(total))
                continue;
            gctimes.put(gc.getName(), total);
            Long duration = total - previousTotal; // may be zero for a really fast collection

            Long previousCount = gccounts.get(gc.getName());
            Long count = gc.getCollectionCount();

            if (previousCount == null)
                previousCount = 0L;
            if (count.equals(previousCount))
                continue;

            gccounts.put(gc.getName(), count);

            MemoryUsage mu = membean.getHeapMemoryUsage();
            long memoryUsed = mu.getUsed();
            long memoryMax = mu.getMax();

            long durationPerCollection = duration / (count - previousCount);
            if (durationPerCollection > MIN_DURATION) {
                String st = String.format("GC for %s: %s ms for %s collections, %s used; max is %s",
                        gc.getName(), duration, count - previousCount, memoryUsed, memoryMax);
                logger.info(st);
            } else if (logger.isDebugEnabled()) {
                String st = String.format("GC for %s: %s ms for %s collections, %s used; max is %s",
                        gc.getName(), duration, count - previousCount, memoryUsed, memoryMax);
                logger.debug(st);
            }

            // if we just finished a full collection and we're still using a lot of memory, log
            if (oldGenGCs.contains(gc.getName()))
            {
                double usage = (double) memoryUsed / memoryMax;

                if (memoryUsed > .5 * memoryMax)
                {
                    logger.warn("Heap is " + usage + " full out of " + memoryMax + ".");
                }
            }
        }
    }

    public static void main(String args[]) throws Exception {
        ArrayList<byte[]> stuff = new ArrayList<byte[]>();
        GCInspector.instance.start(new ScheduledThreadPoolExecutor(1));
        int allocations = 3000;
        for (int ii = 0; ii < allocations; ii++) {
            for (int zz = 0; zz < 1024; zz++) {
                stuff.add(new byte[1024]);
            }
        }
        Random r = new Random();
        while (true) {
            stuff.set(r.nextInt(stuff.size()), new byte[1024]);
        }
    }
}
