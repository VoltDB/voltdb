/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.export;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import org.junit.Test;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * @author rdykiel
 *
 */
public class TestExecutorFactory {

    private static final int SITES_COUNT = 8;
    private static final int MAX_PARTITION = (3 * SITES_COUNT) / 2;

    /**
     * Common test case verifying max number of executors allocated and their release
     *
     * @param maxRequested max number of threads requested (simulates MAX_EXPORT_THREADS)
     * @param maxExpected max number of threads expected created
     * @throws Exception
     */
    public void commonSimpleTest(int maxRequested, int maxExpected) throws Exception {

        ExecutorFactory tested = new ExecutorFactory() {
            @Override
            int getMinThreads() {
                return 1;
            }
            @Override
            Integer getConfigMaxThreads() {
                return maxRequested;
            }
            @Override
            int getLocalSitesCount() {
                return SITES_COUNT;
            }
       };

       // verify we're using maxExpected executors for MAX_PARTITION allocations
       IdentityHashMap<ListeningExecutorService, Object> execs = new IdentityHashMap<>();
       for (int i = 0; i < MAX_PARTITION; i++) {

           ListeningExecutorService lesAlloc = tested.getExecutor(i, "FOO-" + i);
           if (!execs.containsKey(lesAlloc)) {
               execs.put(lesAlloc, null);
           }
       }
       assertTrue(tested.getMaxThreadCount() == maxExpected);
       assertTrue(tested.getCurrentThreadCount() == execs.size());

       // Verify release based on refCount
       for (int i = MAX_PARTITION - 1; i >= 0; i--) {
           tested.freeExecutor(i,  "_FOO-" + i);
           if (i > 0) {
               // Executors must still be allocated
               assertTrue(tested.getCurrentThreadCount() != 0);
           }
           else {
               // All executor must have been released
               assertTrue(tested.getCurrentThreadCount() == 0);
           }
       }
    }

    /**
     * Common partition test case: allocates 3 * MAX_PARTITION and verifies pid -> executor is constant
     *
     * @param maxRequested max number of threads requested (simulates MAX_EXPORT_THREADS)
     * @param maxExpected max number of threads expected created
     * @throws Exception
     */
    public void commonPartitionTest(int maxRequested, int maxExpected) throws Exception {

        ExecutorFactory tested = new ExecutorFactory() {
            @Override
            Integer getConfigMaxThreads() {
                return maxRequested;
            }
            @Override
            int getLocalSitesCount() {
                return SITES_COUNT;
            }
       };

       IdentityHashMap<ListeningExecutorService, Object> execs = new IdentityHashMap<>();
       Map<Integer, ListeningExecutorService> pidMap = new HashMap<>();

       for (int i = 0; i < MAX_PARTITION; i++) {

           // Allocate executor for partition
           ListeningExecutorService lesAlloc = tested.getExecutor(i, "FOO-" + i);
           if (!execs.containsKey(lesAlloc)) {
               execs.put(lesAlloc, null);
           }
           pidMap.put(i, lesAlloc);

           // Allocate second time for same pid and verify same gets executor allocated
           // (we may be below the max number of threads)
           ListeningExecutorService lesAlloc2 = tested.getExecutor(i, "FOO2-" + i);
           assertTrue(lesAlloc == lesAlloc2);
           assertTrue(tested.getCurrentThreadCount() <= tested.getMaxThreadCount());
       }
       assertTrue(tested.getMaxThreadCount() == maxExpected);
       assertTrue(tested.getCurrentThreadCount() == execs.size());

       // Allocate a third time for each partition and verify identical executor
       for (int i = 0; i < MAX_PARTITION; i++) {

           ListeningExecutorService lesAlloc = tested.getExecutor(i, "FOO3-" + i);
           assertTrue(lesAlloc == pidMap.get(i));
       }

       // Verify release based on refCount - first and second release don't release executors
       for (int i = MAX_PARTITION - 1; i >= 0; i--) {
           tested.freeExecutor(i,  "_FOO-" + i);
           tested.freeExecutor(i,  "_FOO2-" + i);
       }
       // All executors must still be allocated
       assertTrue(tested.getCurrentThreadCount() == execs.size());

       // Verify release based on refCount - third release must release all executors
       for (int i = MAX_PARTITION - 1; i >= 0; i--) {
           tested.freeExecutor(i,  "_FOO3-" + i);
       }
       assertTrue(tested.getCurrentThreadCount() == 0);
    }

    /**
     * Test that by default we use only one executor, and the executor release logic
     */
    @Test
    public void testSingleExecutor() throws Exception {

        commonSimpleTest(1, 1);
    }

    /**
     * Test invalid requested thread number reverts to using 1 executor release logic
     */
    @Test
    public void testBadMaxThreads() throws Exception {

        commonSimpleTest(0, 1);
        commonSimpleTest(-1, 1);
    }

    /**
     * Test that max threads are capped by localSitesCount
     * @throws Exception
     */
    @Test
    public void testMaxThreads() throws Exception {

        commonSimpleTest(SITES_COUNT, SITES_COUNT);
        commonSimpleTest(SITES_COUNT + 1, SITES_COUNT);
        commonSimpleTest(2 * SITES_COUNT, SITES_COUNT);
    }

    /**
     * Dummy test somehow: all partitions on same thread - Duh
     */
    @Test
    public void testSingleExecutorPartitions() throws Exception {

        commonPartitionTest(1, 1);
    }

    /**
     * Test various cases of partition allocation
     */
    @Test
    public void testExecutorPartitions() throws Exception {

        commonPartitionTest(SITES_COUNT, SITES_COUNT);
        commonPartitionTest(SITES_COUNT / 2, SITES_COUNT / 2);
    }

}
