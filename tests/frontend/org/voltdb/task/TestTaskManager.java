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

package org.voltdb.task;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

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
import org.voltdb.StatsAgent;
import org.voltdb.StatsSelector;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Task;
import org.voltdb.catalog.TaskParameter;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.deploymentfile.TaskSettingsType;
import org.voltdb.utils.InMemoryJarfile;

import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

public class TestTaskManager {

    static AtomicInteger s_firstSchedulerCallCount = new AtomicInteger();
    static AtomicInteger s_postRunSchedulerCallCount = new AtomicInteger();

    private static final String PROCEDURE_NAME = "SomeProcedure";

    @Rule
    public final TestName m_name = new TestName();

    private AuthSystem m_authSystem;
    private ClientInterface m_clientInterface;
    private InternalConnectionHandler m_internalConnectionHandler;
    private TaskManager m_taskManager;
    private Database m_database;
    private Procedure m_procedure = new Procedure();
    private StatsAgent m_statsAgent = new StatsAgent();
    private TaskSettingsType m_schedulesConfig = new TaskSettingsType();
    private ClientResponse m_response;

    @Before
    public void setup() {
        m_authSystem = mock(AuthSystem.class);
        when(m_authSystem.getUser(anyString())).then(m -> mock(AuthUser.class));

        m_internalConnectionHandler = mock(InternalConnectionHandler.class);

        m_response = when(mock(ClientResponse.class).getStatus()).thenReturn(ClientResponse.SUCCESS).getMock();
        when(m_internalConnectionHandler.callProcedure(any(), eq(false), anyInt(), any(), eq(PROCEDURE_NAME), any()))
                .then(m -> {
                    ((ProcedureCallback) m.getArgument(3)).clientCallback(m_response);
                    return true;
                });

        m_clientInterface = mock(ClientInterface.class);
        when(m_clientInterface.getInternalConnectionHandler()).thenReturn(m_internalConnectionHandler);
        when(m_clientInterface.getProcedureFromName(eq(PROCEDURE_NAME))).thenReturn(m_procedure);

        m_taskManager = new TaskManager(m_clientInterface, m_statsAgent, 0);

        s_firstSchedulerCallCount.set(0);
        s_postRunSchedulerCallCount.set(0);

        m_database = new Catalog().getClusters().add("cluster").getDatabases().add("database");
    }

    @After
    public void teardown() throws InterruptedException, ExecutionException {
        m_taskManager.shutdown().get();
    }

    /*
     * Test that a system schedule is only executed when the scheduler is the leader and then is stopped when the
     * configuration is removed
     */
    @Test
    public void systemScheduleCreateDrop() throws Exception {
        Task task = createTask(TestScheduler.class, TaskManager.SCOPE_DATABASE);

        startSync();
        assertEquals(0, s_firstSchedulerCallCount.get());

        processUpdateSync(task);
        assertEquals(0, s_firstSchedulerCallCount.get());

        promoteToLeaderSync(task);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(50);
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
        Task task = createTask(TestScheduler.class, TaskManager.SCOPE_HOSTS);

        m_procedure.setTransactional(false);

        startSync();
        assertEquals(0, s_firstSchedulerCallCount.get());

        processUpdateSync(task);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(50);
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

        Task task = createTask(TestScheduler.class, TaskManager.SCOPE_PARTITIONS);

        m_procedure.setTransactional(true);
        m_procedure.setSinglepartition(true);
        m_procedure.setPartitionparameter(0);
        Column column = new Column();
        column.setType(VoltType.INTEGER.getValue());
        m_procedure.setPartitioncolumn(column);

        startSync();
        assertEquals(0, s_firstSchedulerCallCount.get());

        processUpdateSync(task);
        assertEquals(0, s_firstSchedulerCallCount.get());

        promotedPartitionsSync(0, 4);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(50);
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
        Task task = createTask(TestSchedulerParams.class, TaskManager.SCOPE_DATABASE, 5, "TESTING", "AFFA47");

        startSync();
        assertEquals(0, s_firstSchedulerCallCount.get());

        processUpdateSync(task);
        assertEquals(0, s_firstSchedulerCallCount.get());

        promoteToLeaderSync(task);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(50);
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
        Task task = createTask(TestSchedulerParams.class, TaskManager.SCOPE_DATABASE, 5, "TESTING", "ZZZ");

        assertFalse(m_taskManager.validateTask(task, getClass().getClassLoader()).isValid());

        task.getParameters().get("0").setParameter("NAN");
        task.getParameters().get("2").setParameter("7894");
        assertFalse(m_taskManager.validateTask(task, getClass().getClassLoader()).isValid());

        task.setSchedulerclass(TestScheduler.class.getName());
        assertFalse(m_taskManager.validateTask(task, getClass().getClassLoader()).isValid());
    }

    /*
     * Test that the scheduler manager can succesfully be shutdown with active schedules running
     */
    @Test
    public void shutdownWithSchedulesActive() throws Exception {
        TheHashinator.initialize(ElasticHashinator.class, new ElasticHashinator(6).getConfigBytes());

        Task task1 = createTask(TestScheduler.class, TaskManager.SCOPE_DATABASE);
        Task task2 = createTask(m_name.getMethodName() + "_p", TestScheduler.class, TaskManager.SCOPE_PARTITIONS);

        m_procedure.setTransactional(true);
        m_procedure.setSinglepartition(true);
        m_procedure.setPartitionparameter(0);
        Column column = new Column();
        column.setType(VoltType.INTEGER.getValue());
        m_procedure.setPartitioncolumn(column);

        startSync(task1, task2);
        promoteToLeaderSync(task1, task2);
        promotedPartitionsSync(0, 1, 2, 3);
        Thread.sleep(50);
        assertEquals(5, s_firstSchedulerCallCount.get());
        m_taskManager.shutdown().get();
    }

    /*
     * Test that a scheduler can just schedule itself to be rerun multple times
     */
    @Test
    public void rerunScheduler() throws Exception {
        Task task = createTask(TestSchedulerRerun.class, TaskManager.SCOPE_DATABASE, 5);

        startSync(task);
        promoteToLeaderSync(task);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(50);
        assertEquals(1, s_firstSchedulerCallCount.get());
        assertEquals(4, s_postRunSchedulerCallCount.get());
    }

    /*
     * Test that stats are maintained while a previously running procedure is disabled but scheduler is restarted when
     * enabled
     */
    @Test
    public void disableReenableScheduler() throws Exception {
        Task task = createTask(TestScheduler.class, TaskManager.SCOPE_DATABASE);

        startSync();
        promoteToLeaderSync(task);
        Thread.sleep(50);
        assertEquals(1, s_firstSchedulerCallCount.get());

        task.setEnabled(false);
        processUpdateSync(task);
        Thread.sleep(5);
        validateStats("DISABLED", null);

        task.setEnabled(true);
        processUpdateSync(task);
        Thread.sleep(20);
        dropScheduleAndAssertCounts(2);
    }

    /*
     * Test that newly promoted partitions honor the enable flag of a procedure
     */
    @Test
    public void partitionPromotionAndDisabledSchedules() throws Exception {
        Task task = createTask(TestSchedulerRerun.class, TaskManager.SCOPE_PARTITIONS, 5);

        startSync(task);

        task.setEnabled(false);
        processUpdateSync(task);

        promotedPartitionsSync(0, 1);

        assertEquals(0, getScheduleStats().getRowCount());

        task.setEnabled(true);
        processUpdateSync(task);

        assertEquals(2, getScheduleStats().getRowCount());

        promotedPartitionsSync(2, 3);
        assertEquals(4, getScheduleStats().getRowCount());

        task.setEnabled(false);
        processUpdateSync(task);

        promotedPartitionsSync(4, 5);
        assertEquals(4, getScheduleStats().getRowCount());

        task.setEnabled(true);
        processUpdateSync(task);

        assertEquals(6, getScheduleStats().getRowCount());

        task.setEnabled(false);
        processUpdateSync(task);

        demotedPartitionsSync(3, 4, 5);
        assertEquals(3, getScheduleStats().getRowCount());
    }

    /*
     * Test that minimum delay configuration is honored
     */
    @Test
    public void minDelay() throws Exception {
        m_schedulesConfig.setMinDelayMs(10000);
        Task task = createTask(TestScheduler.class, TaskManager.SCOPE_DATABASE);
        startSync();
        promoteToLeaderSync(task);
        Thread.sleep(50);
        assertEquals(1, s_firstSchedulerCallCount.get());
        assertEquals(0, s_postRunSchedulerCallCount.get());
    }

    /*
     * Test that max run frequency configuration is honored
     */
    @Test
    public void maxRunFrequency() throws Exception {
        m_schedulesConfig.setMaxRunFrequency(1.0);
        Task task = createTask(TestScheduler.class, TaskManager.SCOPE_DATABASE);
        startSync();
        promoteToLeaderSync(task);
        Thread.sleep(50);
        assertEquals(1, s_firstSchedulerCallCount.get());
        assertEquals(1, s_postRunSchedulerCallCount.get());
    }

    /*
     * Test that schedules are only restarted when classes are updated and only if the class or one of its deps change
     *
     * TODO test changing the class restarts the schedule
     */
    @Test
    public void relaodWithInMemoryJarFile() throws Exception {
        InMemoryJarfile jarFile = new InMemoryJarfile();
        VoltCompiler vc = new VoltCompiler(false);
        vc.addClassToJar(jarFile, TestTaskManager.class);

        Task task1 = createTask("TestScheduler", TestScheduler.class, TaskManager.SCOPE_DATABASE);
        Task task2 = createTask("TestSchedulerRerun", TestSchedulerRerun.class, TaskManager.SCOPE_DATABASE,
                Integer.MAX_VALUE);

        startSync();
        promoteToLeaderSync();
        processUpdateSync(jarFile.getLoader(), false, task1, task2);

        Thread.sleep(30);

        VoltTable table = getScheduleStats();
        Map<String, Long> invocationCounts = new HashMap<>();
        while (table.advanceRow()) {
            invocationCounts.put(table.getString("NAME"), table.getLong("SCHEDULER_INVOCATIONS"));
        }

        // No schedules should restart since class and deps did not change
        processUpdateSync(jarFile.getLoader(), false, task1, task2);
        Thread.sleep(5);

        table = getScheduleStats();
        while (table.advanceRow()) {
            String scheduleName = table.getString("NAME");
            long currentCount = table.getLong("SCHEDULER_INVOCATIONS");
            assertTrue("Count decreased for " + scheduleName,
                    invocationCounts.put(scheduleName, currentCount) < currentCount);
        }

        Thread.sleep(5);

        // Only schedules which do not specify deps should restart
        processUpdateSync(jarFile.getLoader(), true, task1, task2);
        Thread.sleep(5);

        table = getScheduleStats();
        while (table.advanceRow()) {
            String scheduleName = table.getString("NAME");
            long currentCount = table.getLong("SCHEDULER_INVOCATIONS");
            long previousCount = invocationCounts.put(scheduleName, currentCount);
            if (scheduleName.equals("TestScheduler")) {
                assertTrue("Count decreased for " + scheduleName, previousCount < currentCount);
            } else {
                assertTrue("Count increased for " + scheduleName + " from " + previousCount + " to " + currentCount,
                        previousCount * 3 / 2 > currentCount);
            }
        }

        Thread.sleep(5);

        // Update class dep so all should restart
        vc = new VoltCompiler(false);
        jarFile = new InMemoryJarfile();
        vc.addClassToJar(jarFile, TestTaskManager.class);
        jarFile.removeClassFromJar(TestSchedulerParams.class.getName());
        processUpdateSync(jarFile.getLoader(), true, task1, task2);
        Thread.sleep(5);

        table = getScheduleStats();
        while (table.advanceRow()) {
            String scheduleName = table.getString("NAME");
            long currentCount = table.getLong("SCHEDULER_INVOCATIONS");
            long previousCount = invocationCounts.put(scheduleName, currentCount);
            assertTrue("Count increased for " + scheduleName + " from " + previousCount + " to " + currentCount,
                    previousCount * 3 / 2 > currentCount);
        }
    }

    /*
     * Test that the onErrot value can be modified on a running schedule
     */
    @Test
    public void changeOnErrorWhileRunning() throws Exception {
        when(m_response.getStatus()).thenReturn(ClientResponse.USER_ABORT);
        Task task = createTask(TestScheduler.class, TaskManager.SCOPE_PARTITIONS);
        m_procedure.setSinglepartition(true);
        task.setOnerror("IGNORE");

        startSync(task);
        promotedPartitionsSync(0, 1, 2, 3, 4, 5);

        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(50);
        assertEquals(6, s_firstSchedulerCallCount.get());
        assertTrue("Scheduler should have been called at least once: " + s_postRunSchedulerCallCount.get(),
                s_postRunSchedulerCallCount.get() > 0);

        validateStats("RUNNING", r -> assertNull(r.getString("SCHEDULER_STATUS")));

        task.setOnerror("ABORT");
        processUpdateSync(task);
        Thread.sleep(5);

        validateStats("ERROR", r -> assertTrue(r.getString("SCHEDULER_STATUS").startsWith("Procedure ")));
    }

    /*
     * Test that the validate parameters method is being called correctly
     */
    @Test
    public void testValidateParameters() throws Exception {
        Task task = createTask(TestSchedulerValidateParams.class, TaskManager.SCOPE_HOSTS, new Object[1]);

        assertTrue(m_taskManager.validateTask(task, getClass().getClassLoader()).isValid());

        task.setSchedulerclass(TestSchedulerValidateParamsWithHelper.class.getName());
        assertTrue(m_taskManager.validateTask(task, getClass().getClassLoader()).isValid());

        // Parameter fails validation
        task.getParameters().get("0").setParameter("FAIL");
        assertFalse(m_taskManager.validateTask(task, getClass().getClassLoader()).isValid());

        task.setSchedulerclass(TestSchedulerValidateParams.class.getName());
        assertFalse(m_taskManager.validateTask(task, getClass().getClassLoader()).isValid());
    }

    private void dropScheduleAndAssertCounts() throws Exception {
        dropScheduleAndAssertCounts(1);
    }

    private void dropScheduleAndAssertCounts(int startCount) throws Exception {
        validateStats();
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

    private Task createTask(Class<? extends Scheduler> clazz, String scope, Object... params) {
        return createTask(m_name.getMethodName(), clazz, scope, params);
    }

    private Task createTask(String scheduleName, Class<? extends Scheduler> clazz, String scope, Object... params) {
        Task task = m_database.getTasks().add(scheduleName);
        task.setEnabled(true);
        task.setName(scheduleName);
        task.setScope(scope);
        task.setSchedulerclass(clazz.getName());
        task.setUser("user");
        task.setOnerror("ABORT");
        CatalogMap<TaskParameter> paramMap = task.getParameters();
        for (int i = 0; i < params.length; ++i) {
            TaskParameter tp = paramMap.add(Integer.toString(i));
            tp.setIndex(i);
            tp.setParameter(params[i] == null ? null : params[i].toString());
        }
        return task;
    }

    private void startSync(Task... tasks) throws InterruptedException, ExecutionException {
        m_taskManager.start(m_schedulesConfig, Arrays.asList(tasks), m_authSystem, getClass().getClassLoader())
                .get();
    }

    public void promoteToLeaderSync(Task... tasks) throws InterruptedException, ExecutionException {
        m_taskManager
                .promoteToLeader(m_schedulesConfig, Arrays.asList(tasks), m_authSystem, getClass().getClassLoader())
                .get();
    }

    private void processUpdateSync(Task... tasks) throws InterruptedException, ExecutionException {
        processUpdateSync(getClass().getClassLoader(), false, tasks);
    }

    private void processUpdateSync(ClassLoader classLoader, boolean classesUpdated, Task... tasks)
            throws InterruptedException, ExecutionException {
        m_taskManager
                .processUpdate(m_schedulesConfig, Arrays.asList(tasks), m_authSystem, classLoader, classesUpdated)
                .get();
    }

    private void promotedPartitionsSync(int... partitionIds) throws InterruptedException, ExecutionException {
        List<ListenableFuture<?>> futures = new ArrayList<>(partitionIds.length);
        for (int partitionId : partitionIds) {
            futures.add(m_taskManager.promotedPartition(partitionId));
        }
        Futures.whenAllSucceed(futures).call(() -> null).get();
    }

    private void demotedPartitionsSync(int... partitionIds) throws InterruptedException, ExecutionException {
        List<ListenableFuture<?>> futures = new ArrayList<>(partitionIds.length);
        for (int partitionId : partitionIds) {
            futures.add(m_taskManager.demotedPartition(partitionId));
        }
        Futures.whenAllSucceed(futures).call(() -> null).get();
    }

    private VoltTable getScheduleStats() {
        return m_statsAgent.getStatsAggregate(StatsSelector.TASK, false, System.currentTimeMillis());
    }

    private void validateStats() {
        validateStats("RUNNING", null);
    }

    private void validateStats(String state, Consumer<VoltTableRow> validator) {
        VoltTable table = getScheduleStats();
        long totalSchedulerInvocations = 0;
        long totalProcedureInvocations = 0;
        while (table.advanceRow()) {
            if (validator != null) {
                validator.accept(table);
            }
            assertEquals(state, table.getString("STATE"));
            totalSchedulerInvocations += table.getLong("SCHEDULER_INVOCATIONS");
            totalProcedureInvocations += table.getLong("PROCEDURE_INVOCATIONS");
        }

        assertTrue(totalSchedulerInvocations >= totalProcedureInvocations);
        assertTrue(totalSchedulerInvocations <= s_firstSchedulerCallCount.get() + s_postRunSchedulerCallCount.get());
    }

    public static class TestScheduler implements Scheduler {
        @Override
        public Action getFirstAction() {
            s_firstSchedulerCallCount.getAndIncrement();
            return Action.createProcedure(100, TimeUnit.MICROSECONDS, PROCEDURE_NAME);
        }

        @Override
        public Action getNextAction(ActionResult previousProcedureRun) {
            s_postRunSchedulerCallCount.getAndIncrement();
            return Action.createProcedure(100, TimeUnit.MICROSECONDS, PROCEDURE_NAME);
        }

        @Override
        public Collection<String> getDependencies() {
            return Collections.singleton(TestSchedulerParams.class.getName());
        }
    }

    public static class TestSchedulerParams implements Scheduler {
        public void initialize(int arg1, String arg2, byte[] arg3) {}

        @Override
        public Action getFirstAction() {
            s_firstSchedulerCallCount.getAndIncrement();
            return Action.createProcedure(100, TimeUnit.MICROSECONDS, PROCEDURE_NAME);
        }

        @Override
        public Action getNextAction(ActionResult previousProcedureRun) {
            s_postRunSchedulerCallCount.getAndIncrement();
            return Action.createProcedure(100, TimeUnit.MICROSECONDS, PROCEDURE_NAME);
        }
    }

    public static class TestSchedulerRerun implements Scheduler {
        private int m_maxRunCount;
        private int m_runCount = 0;

        public void initialize(int maxRunCount) {
            m_maxRunCount = maxRunCount;
        }

        @Override
        public Action getFirstAction() {
            s_firstSchedulerCallCount.getAndIncrement();
            return ++m_runCount < m_maxRunCount ? Action.createRerun(100, TimeUnit.MICROSECONDS)
                    : Action.createExit(null);
        }

        @Override
        public Action getNextAction(ActionResult previousProcedureRun) {
            assertNull(previousProcedureRun.getProcedure());
            s_postRunSchedulerCallCount.getAndIncrement();

            return ++m_runCount < m_maxRunCount ? Action.createRerun(100, TimeUnit.MICROSECONDS)
                    : Action.createExit(null);
        }
    }

    public static class TestSchedulerValidateParams implements Scheduler {
        public static String validateParameters(String value) {
            return value;
        }

        public void initialize(String value) {}

        @Override
        public Action getFirstAction() {
            return null;
        }

        @Override
        public Action getNextAction(ActionResult result) {
            return null;
        }
    }

    public static class TestSchedulerValidateParamsWithHelper extends TestSchedulerValidateParams {
        public static String validateParameters(TaskHelper helper, String value) {
            return TestSchedulerValidateParams.validateParameters(value);
        }

        public void initialize(TaskHelper helper, String value) {
            super.initialize(value);
        }
    }
}
