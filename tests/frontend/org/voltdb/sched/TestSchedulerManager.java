/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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

package org.voltdb.sched;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.voltdb.AuthSystem;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.ClientInterface;
import org.voltdb.ElasticHashinator;
import org.voltdb.InternalConnectionHandler;
import org.voltdb.TheHashinator;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.ProcedureSchedule;
import org.voltdb.catalog.SchedulerParam;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

public class TestSchedulerManager {

    static AtomicInteger s_firstSchedulerCallCount = new AtomicInteger();
    static AtomicInteger s_postRunSchedulerCallCount = new AtomicInteger();

    private static final String PROCEDURE_NAME = "SomeProcedure";

    @Rule
    public final TestName name = new TestName();

    private AuthSystem m_authSystem;
    private ClientInterface m_clientInterface;
    private InternalConnectionHandler m_internalConnectionHandler;
    private SchedulerManager m_schedulerManager;
    private Database m_database;
    private Procedure m_procedure = new Procedure();

    @Before
    public void setup() {
        m_authSystem = mock(AuthSystem.class);
        when(m_authSystem.getUser(anyString())).then(m -> mock(AuthUser.class));

        m_internalConnectionHandler = mock(InternalConnectionHandler.class);
        when(m_internalConnectionHandler.callProcedure(any(), eq(false), anyInt(), any(), eq(PROCEDURE_NAME), any()))
                .then(m -> {
                    ((ProcedureCallback) m.getArgument(3)).clientCallback(mock(ClientResponse.class));
                    return true;
                });

        m_clientInterface = mock(ClientInterface.class);
        when(m_clientInterface.getInternalConnectionHandler()).thenReturn(m_internalConnectionHandler);
        when(m_clientInterface.getProcedureFromName(eq(PROCEDURE_NAME))).thenReturn(m_procedure);

        m_schedulerManager = new SchedulerManager(m_clientInterface, 0);

        s_firstSchedulerCallCount.set(0);
        s_postRunSchedulerCallCount.set(0);

        m_database = new Catalog().getClusters().add("cluster").getDatabases().add("database");
    }

    @After
    public void teardown() throws InterruptedException, ExecutionException {
        m_schedulerManager.shutdown().get();
    }

    /*
     * Test that a system schedule is only executed when the scheduler is the leader and then is stopped when the
     * configuration is removed
     */
    @Test
    public void systemScheduleCreateDrop() throws Exception {
        ProcedureSchedule schedule = createProcedureSchedule(TestScheduler.class, SchedulerManager.RUN_LOCATION_SYSTEM);

        startSync();
        assertEquals(0, s_firstSchedulerCallCount.get());

        processUpdateSync(schedule);
        assertEquals(0, s_firstSchedulerCallCount.get());

        promoteToLeaderSync(schedule);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(100);
        assertEquals(1, s_firstSchedulerCallCount.get());
        assertTrue("Scheduler should have been called at least once: " + s_postRunSchedulerCallCount.get(),
                s_postRunSchedulerCallCount.get() > 0);

        dropScheduleAndAssertCounts();
    }

    /*
     * Test that a host schedule is once configured and then is stopped when the configuration is removed
     */
    @Test
    public void hostScheduleCreateDrop() throws Exception {
        ProcedureSchedule schedule = createProcedureSchedule(TestScheduler.class, SchedulerManager.RUN_LOCATION_HOSTS);

        m_procedure.setTransactional(false);

        startSync();
        assertEquals(0, s_firstSchedulerCallCount.get());

        processUpdateSync(schedule);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(100);
        assertEquals(1, s_firstSchedulerCallCount.get());
        assertTrue("Scheduler should have been called at least once: " + s_postRunSchedulerCallCount.get(),
                s_postRunSchedulerCallCount.get() > 0);

        dropScheduleAndAssertCounts();
    }

    /*
     * Test that a partition schedule is only executed when the host is a leader of a partition and then is stopped when
     * the partition is demoted or configuration is removed
     */
    @Test
    public void partitionScheduleCreateDrop() throws Exception {
        TheHashinator.initialize(ElasticHashinator.class, new ElasticHashinator(6).getConfigBytes());

        ProcedureSchedule schedule = createProcedureSchedule(TestScheduler.class,
                SchedulerManager.RUN_LOCATION_PARTITIONS);

        m_procedure.setTransactional(true);
        m_procedure.setSinglepartition(true);
        m_procedure.setPartitionparameter(0);
        Column column = new Column();
        column.setType(VoltType.INTEGER.getValue());
        m_procedure.setPartitioncolumn(column);

        startSync();
        assertEquals(0, s_firstSchedulerCallCount.get());

        processUpdateSync(schedule);
        assertEquals(0, s_firstSchedulerCallCount.get());

        promotedPartitionsSync(0, 4);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(100);
        assertEquals(2, s_firstSchedulerCallCount.get());
        assertTrue("Scheduler should have been called at least once: " + s_postRunSchedulerCallCount.get(),
                s_postRunSchedulerCallCount.get() > 0);

        demotedPartitionsSync(0, 4);
        assertCountsAfterScheduleCanceled(2);

        int previousCount = s_postRunSchedulerCallCount.get();
        promotedPartitionsSync(0);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(20);
        assertTrue("Scheduler should have been called at least " + previousCount + " times: "
                + s_postRunSchedulerCallCount.get(), s_postRunSchedulerCallCount.get() > previousCount);

        dropScheduleAndAssertCounts(3);
    }

    /*
     * Test that a scheduler which takes paramaters can be constructed and executed
     */
    @Test
    public void schedulerWithParameters() throws Exception {
        ProcedureSchedule schedule = createProcedureSchedule(TestSchedulerParams.class,
                SchedulerManager.RUN_LOCATION_SYSTEM, 5, "TESTING", "AFFA47");

        startSync();
        assertEquals(0, s_firstSchedulerCallCount.get());

        processUpdateSync(schedule);
        assertEquals(0, s_firstSchedulerCallCount.get());

        promoteToLeaderSync(schedule);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(100);
        assertEquals(1, s_firstSchedulerCallCount.get());
        assertTrue("Scheduler should have been called at least once: " + s_postRunSchedulerCallCount.get(),
                s_postRunSchedulerCallCount.get() > 0);

        dropScheduleAndAssertCounts();
    }

    /*
     * Test that when bad parameters are passed to a scheduler they are appropriately handled
     */
    @Test
    public void schedulerWithBadParameters() throws Exception {
        ProcedureSchedule schedule = createProcedureSchedule(TestSchedulerParams.class,
                SchedulerManager.RUN_LOCATION_SYSTEM, 5, "TESTING", "ZZZ");

        startSync();
        assertEquals(0, s_firstSchedulerCallCount.get());

        promoteToLeaderSync(schedule);
        assertEquals(0, s_firstSchedulerCallCount.get());

        schedule.getParameters().get("0").setParameter("NAN");
        schedule.getParameters().get("2").setParameter("7894");
        processUpdateSync(schedule);
        assertEquals(0, s_firstSchedulerCallCount.get());

        assertEquals(0, s_postRunSchedulerCallCount.get());
    }

    /*
     * Test that the scheduler manager can succesfully be shutdown with active schedules running
     */
    @Test
    public void shutdownWithSchedulesActive() throws Exception {
        TheHashinator.initialize(ElasticHashinator.class, new ElasticHashinator(6).getConfigBytes());

        ProcedureSchedule schedule1 = createProcedureSchedule(TestScheduler.class,
                SchedulerManager.RUN_LOCATION_SYSTEM);
        ProcedureSchedule schedule2 = createProcedureSchedule(name.getMethodName() + "_p", TestScheduler.class,
                SchedulerManager.RUN_LOCATION_PARTITIONS);

        m_procedure.setTransactional(true);
        m_procedure.setSinglepartition(true);
        m_procedure.setPartitionparameter(0);
        Column column = new Column();
        column.setType(VoltType.INTEGER.getValue());
        m_procedure.setPartitioncolumn(column);

        startSync(schedule1, schedule2);
        promoteToLeaderSync(schedule1, schedule2);
        promotedPartitionsSync(0, 1, 2, 3);
        Thread.sleep(50);
        assertEquals(5, s_firstSchedulerCallCount.get());
        m_schedulerManager.shutdown().get();
    }

    /*
     * Test that a scheduler can just schedule itself to be rerun multple times
     */
    @Test
    public void rerunScheduler() throws Exception {
        ProcedureSchedule schedule = createProcedureSchedule(TestSchedulerRerun.class,
                SchedulerManager.RUN_LOCATION_SYSTEM, 5);

        startSync(schedule);
        promoteToLeaderSync(schedule);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(100);
        assertEquals(1, s_firstSchedulerCallCount.get());
        assertEquals(4, s_postRunSchedulerCallCount.get());
    }

    private void dropScheduleAndAssertCounts() throws Exception {
        dropScheduleAndAssertCounts(1);
    }

    private void dropScheduleAndAssertCounts(int startCount) throws Exception {
        processUpdateSync();
        assertCountsAfterScheduleCanceled(startCount);
    }

    private void assertCountsAfterScheduleCanceled(int startCount) throws InterruptedException {
        int previousCount = s_postRunSchedulerCallCount.get();
        Thread.sleep(10);
        assertEquals(startCount, s_firstSchedulerCallCount.get());
        assertEquals(previousCount, s_postRunSchedulerCallCount.get());

        verify(m_internalConnectionHandler, atLeast(previousCount)).callProcedure(any(), eq(false), anyInt(), any(),
                eq(PROCEDURE_NAME), any());
        verify(m_internalConnectionHandler, atMost(previousCount + startCount)).callProcedure(any(), eq(false),
                anyInt(), any(), eq(PROCEDURE_NAME), any());

        verify(m_clientInterface, atLeast(previousCount)).getProcedureFromName(eq(PROCEDURE_NAME));
        verify(m_clientInterface, atMost(previousCount + startCount)).getProcedureFromName(eq(PROCEDURE_NAME));
    }

    private ProcedureSchedule createProcedureSchedule(Class<? extends Scheduler> clazz, String runLocation,
            Object... params) {
        return createProcedureSchedule(name.getMethodName(), clazz, runLocation, params);
    }

    private ProcedureSchedule createProcedureSchedule(String name, Class<? extends Scheduler> clazz, String runLocation,
            Object... params) {
        ProcedureSchedule ps = m_database.getProcedureschedules().add(name);
        ps.setEnabled(true);
        ps.setName(name);
        ps.setRunlocation(runLocation);
        ps.setSchedulerclass(clazz.getName());
        ps.setUser("user");
        CatalogMap<SchedulerParam> paramMap = ps.getParameters();
        for (int i = 0;i<params.length;++i) {
            SchedulerParam sp = paramMap.add(Integer.toString(i));
            sp.setIndex(i);
            sp.setParameter(params[i].toString());
        }
        return ps;
    }

    private void startSync(ProcedureSchedule... schedules) throws InterruptedException, ExecutionException {
        m_schedulerManager.start(Arrays.asList(schedules), m_authSystem, getClass().getClassLoader()).get();
    }

    public void promoteToLeaderSync(ProcedureSchedule... schedules) throws InterruptedException, ExecutionException {
        m_schedulerManager.promoteToLeader(Arrays.asList(schedules), m_authSystem, getClass().getClassLoader()).get();
    }

    private void processUpdateSync(ProcedureSchedule... schedules) throws InterruptedException, ExecutionException {
        m_schedulerManager.processUpdate(Arrays.asList(schedules), m_authSystem, getClass().getClassLoader()).get();
    }

    private void promotedPartitionsSync(int... partitionIds) throws InterruptedException, ExecutionException {
        List<ListenableFuture<?>> futures = new ArrayList<>(partitionIds.length);
        for (int partitionId : partitionIds) {
            futures.add(m_schedulerManager.promotedPartition(partitionId));
        }
        Futures.whenAllSucceed(futures).call(() -> null).get();
    }

    private void demotedPartitionsSync(int... partitionIds) throws InterruptedException, ExecutionException {
        List<ListenableFuture<?>> futures = new ArrayList<>(partitionIds.length);
        for (int partitionId : partitionIds) {
            futures.add(m_schedulerManager.demotedPartition(partitionId));
        }
        Futures.whenAllSucceed(futures).call(() -> null).get();
    }

    public static class TestScheduler implements Scheduler {
        @Override
        public SchedulerResult nextRun(ScheduledProcedure previousProcedureRun) {
            if (previousProcedureRun == null) {
                s_firstSchedulerCallCount.getAndIncrement();
            } else {
                s_postRunSchedulerCallCount.getAndIncrement();
            }
            return SchedulerResult.scheduleProcedure(1, TimeUnit.MICROSECONDS, PROCEDURE_NAME);
        }
    }

    public static class TestSchedulerParams implements Scheduler {
        public TestSchedulerParams(int arg1, String arg2, byte[] arg3) {}

        @Override
        public SchedulerResult nextRun(ScheduledProcedure previousProcedureRun) {
            if (previousProcedureRun == null) {
                s_firstSchedulerCallCount.getAndIncrement();
            } else {
                s_postRunSchedulerCallCount.getAndIncrement();
            }
            return SchedulerResult.scheduleProcedure(1, TimeUnit.MICROSECONDS, PROCEDURE_NAME);
        }
    }

    public static class TestSchedulerRerun implements Scheduler {
        private final int m_maxRunCount;
        private int m_runCount = 0;

        public TestSchedulerRerun(int maxRunCount) {
            m_maxRunCount = maxRunCount;
        }

        @Override
        public SchedulerResult nextRun(ScheduledProcedure previousProcedureRun) {
            if (previousProcedureRun == null) {
                s_firstSchedulerCallCount.getAndIncrement();
            } else {
                assertNull(previousProcedureRun.getProcedure());
                s_postRunSchedulerCallCount.getAndIncrement();
            }

            return ++m_runCount < m_maxRunCount ? SchedulerResult.rerun(1, TimeUnit.MICROSECONDS)
                    : SchedulerResult.exit(null);
        }
    }
}
