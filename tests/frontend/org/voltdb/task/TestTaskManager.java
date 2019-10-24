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
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
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
import java.util.concurrent.ThreadLocalRandom;
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
import org.voltdb.task.TaskManager.TaskValidationResult;
import org.voltdb.utils.InMemoryJarfile;

import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

public class TestTaskManager {

    static AtomicInteger s_firstActionSchedulerCallCount = new AtomicInteger();
    static AtomicInteger s_postRunActionSchedulerCallCount = new AtomicInteger();

    private static final String PROCEDURE_NAME = "SomeProcedure";
    private static final String USER_NAME = "user";

    @Rule
    public final TestName m_name = new TestName();

    private boolean m_readOnly;
    private AuthSystem m_authSystem;
    private ClientInterface m_clientInterface;
    private InternalConnectionHandler m_internalConnectionHandler;
    private TaskManager m_taskManager;
    private Database m_database;
    private Procedure m_procedure;
    private StatsAgent m_statsAgent = new StatsAgent();
    private TaskSettingsType m_schedulesConfig = new TaskSettingsType();
    private ClientResponse m_response;
    private int m_taskNumber = 0;

    @Before
    public void setup() {
        m_database = new Catalog().getClusters().add("cluster").getDatabases().add("database");
        m_database.getUsers().add(USER_NAME);
        m_procedure = m_database.getProcedures().add(PROCEDURE_NAME);

        m_authSystem = mock(AuthSystem.class);
        when(m_authSystem.getUser(anyString())).then(m -> mock(AuthUser.class));

        m_internalConnectionHandler = mock(InternalConnectionHandler.class);

        m_response = when(mock(ClientResponse.class).getStatus()).thenReturn(ClientResponse.SUCCESS).getMock();
        when(m_internalConnectionHandler.callProcedure(any(), any(), eq(false), any(), eq(m_procedure), any(),
                eq(false), any())).then(m -> {
                    ((ProcedureCallback) m.getArgument(5)).clientCallback(m_response);
                    return true;
                });

        m_clientInterface = mock(ClientInterface.class);
        when(m_clientInterface.getInternalConnectionHandler()).thenReturn(m_internalConnectionHandler);
        when(m_clientInterface.getProcedureFromName(eq(PROCEDURE_NAME))).thenReturn(m_procedure);

        m_taskManager = new TaskManager(m_clientInterface, m_statsAgent, 0, false, () -> m_readOnly);

        s_firstActionSchedulerCallCount.set(0);
        s_postRunActionSchedulerCallCount.set(0);
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
        Task task = createTask(TestActionScheduler.class, TaskScope.DATABASE);

        startSync();
        assertEquals(0, s_firstActionSchedulerCallCount.get());

        processUpdateSync(task);
        assertEquals(0, s_firstActionSchedulerCallCount.get());

        promoteToLeaderSync(task);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(50);
        assertEquals(1, s_firstActionSchedulerCallCount.get());
        assertTrue("ActionScheduler should have been called at least once: " + s_postRunActionSchedulerCallCount.get(),
                s_postRunActionSchedulerCallCount.get() > 0);

        dropScheduleAndAssertCounts();
    }

    /*
     * Test that a host schedule is once configured and then is stopped when the configuration is removed
     */
    @Test
    public void hostScheduleCreateDrop() throws Exception {
        Task task = createTask(TestActionScheduler.class, TaskScope.HOSTS);

        m_procedure.setTransactional(false);

        startSync();
        assertEquals(0, s_firstActionSchedulerCallCount.get());

        processUpdateSync(task);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(50);
        assertEquals(1, s_firstActionSchedulerCallCount.get());
        assertTrue("ActionScheduler should have been called at least once: " + s_postRunActionSchedulerCallCount.get(),
                s_postRunActionSchedulerCallCount.get() > 0);

        dropScheduleAndAssertCounts();
    }

    /*
     * Test that a partition schedule is only executed when the host is a leader of a partition and then is stopped when
     * the partition is demoted or configuration is removed
     */
    @Test
    public void partitionScheduleCreateDrop() throws Exception {
        Task task = createTask(TestActionScheduler.class, TaskScope.PARTITIONS);

        m_procedure.setTransactional(true);
        m_procedure.setSinglepartition(true);
        m_procedure.setPartitionparameter(-1);

        startSync();
        assertEquals(0, s_firstActionSchedulerCallCount.get());

        processUpdateSync(task);
        assertEquals(0, s_firstActionSchedulerCallCount.get());

        promotedPartitionsSync(0, 4);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(50);
        assertEquals(2, s_firstActionSchedulerCallCount.get());
        assertTrue("ActionScheduler should have been called at least once: " + s_postRunActionSchedulerCallCount.get(),
                s_postRunActionSchedulerCallCount.get() > 0);

        demotedPartitionsSync(0, 4);
        assertCountsAfterScheduleCanceled(2, 0);

        int previousCount = s_postRunActionSchedulerCallCount.get();
        promotedPartitionsSync(0);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(20);
        assertTrue(
                "ActionScheduler should have been called at least " + previousCount + " times: "
                        + s_postRunActionSchedulerCallCount.get(),
                s_postRunActionSchedulerCallCount.get() > previousCount);

        dropScheduleAndAssertCounts(3);
    }

    /*
     * Test that a scheduler which takes paramaters can be constructed and executed
     */
    @Test
    public void schedulerWithParameters() throws Exception {
        Task task = createTask(TestActionSchedulerParams.class, TaskScope.DATABASE, 5, "TESTING", "AFFA47");

        startSync();
        assertEquals(0, s_firstActionSchedulerCallCount.get());

        processUpdateSync(task);
        assertEquals(0, s_firstActionSchedulerCallCount.get());

        promoteToLeaderSync(task);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(50);
        assertEquals(1, s_firstActionSchedulerCallCount.get());
        assertTrue("ActionScheduler should have been called at least once: " + s_postRunActionSchedulerCallCount.get(),
                s_postRunActionSchedulerCallCount.get() > 0);

        dropScheduleAndAssertCounts();
    }

    /*
     * Test that when bad parameters are passed to a scheduler they are appropriately handled
     */
    @Test
    public void schedulerWithBadParameters() throws Exception {
        Task task = createTask(TestActionSchedulerParams.class, TaskScope.DATABASE, 5, "TESTING", "ZZZ");

        assertFalse(validateTask(task).isValid());

        task.getSchedulerparameters().get("0").setParameter("NAN");
        task.getSchedulerparameters().get("2").setParameter("7894");
        assertFalse(validateTask(task).isValid());

        task.setSchedulerclass(TestActionScheduler.class.getName());
        assertFalse(validateTask(task).isValid());
    }

    /*
     * Test that the scheduler manager can succesfully be shutdown with active schedules running
     */
    @Test
    public void shutdownWithSchedulesActive() throws Exception {
        TheHashinator.initialize(ElasticHashinator.class, new ElasticHashinator(6).getConfigBytes());

        Task task1 = createTask(TestActionScheduler.class, TaskScope.DATABASE);
        Task task2 = createTask(TestActionScheduler.class, TaskScope.PARTITIONS);

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
        assertEquals(5, s_firstActionSchedulerCallCount.get());
        m_taskManager.shutdown().get();
    }

    /*
     * Test that a scheduler can just schedule itself to be rerun multple times
     */
    @Test
    public void rerunActionScheduler() throws Exception {
        Task task = createTask(TestActionSchedulerRerun.class, TaskScope.DATABASE, 5);

        startSync(task);
        promoteToLeaderSync(task);
        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(50);
        assertEquals(1, s_firstActionSchedulerCallCount.get());
        assertEquals(4, s_postRunActionSchedulerCallCount.get());
    }

    /*
     * Test that stats are maintained while a previously running procedure is disabled but scheduler is restarted when
     * enabled
     */
    @Test
    public void disableReenableActionScheduler() throws Exception {
        Task task = createTask(TestActionScheduler.class, TaskScope.DATABASE);

        startSync();
        promoteToLeaderSync(task);
        Thread.sleep(50);
        assertEquals(1, s_firstActionSchedulerCallCount.get());

        task.setEnabled(false);
        processUpdateSync(task);
        Thread.sleep(5);
        validateStats(1, "DISABLED", null);

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
        Task task = createTask(TestActionSchedulerRerun.class, TaskScope.PARTITIONS, 5);

        startSync(task);

        task.setEnabled(false);
        processUpdateSync(task);

        promotedPartitionsSync(0, 1);

        assertEquals(2, getScheduleStats().getRowCount());

        task.setEnabled(true);
        processUpdateSync(task);

        assertEquals(2, getScheduleStats().getRowCount());

        promotedPartitionsSync(2, 3);
        assertEquals(4, getScheduleStats().getRowCount());

        task.setEnabled(false);
        processUpdateSync(task);

        promotedPartitionsSync(4, 5);
        assertEquals(6, getScheduleStats().getRowCount());

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
        Task task = createTask(TestActionScheduler.class, TaskScope.DATABASE);
        startSync();
        promoteToLeaderSync(task);
        Thread.sleep(50);
        assertEquals(1, s_firstActionSchedulerCallCount.get());
        assertEquals(0, s_postRunActionSchedulerCallCount.get());
    }

    /*
     * Test that max run frequency configuration is honored
     */
    @Test
    public void maxRunFrequency() throws Exception {
        m_schedulesConfig.setMaxRunFrequency(1.0);
        Task task = createTask(TestActionScheduler.class, TaskScope.DATABASE);
        startSync();
        promoteToLeaderSync(task);
        Thread.sleep(50);
        assertEquals(1, s_firstActionSchedulerCallCount.get());
        assertEquals(1, s_postRunActionSchedulerCallCount.get());
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

        Task task1 = createTask("TestActionScheduler", TestActionScheduler.class, TaskScope.DATABASE);
        Task task2 = createTask("TestActionSchedulerRerun", TestActionSchedulerRerun.class, TaskScope.DATABASE,
                Integer.MAX_VALUE);

        startSync();
        promoteToLeaderSync();
        processUpdateSync(jarFile.getLoader(), false, task1, task2);

        Thread.sleep(100);

        VoltTable table = getScheduleStats();
        Map<String, Long> invocationCounts = new HashMap<>();
        while (table.advanceRow()) {
            invocationCounts.put(table.getString("TASK_NAME"), table.getLong("SCHEDULER_INVOCATIONS"));
        }

        // No schedules should restart since class and deps did not change
        processUpdateSync(jarFile.getLoader(), false, task1, task2);
        Thread.sleep(5);

        table = getScheduleStats();
        while (table.advanceRow()) {
            String scheduleName = table.getString("TASK_NAME");
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
            String scheduleName = table.getString("TASK_NAME");
            long currentCount = table.getLong("SCHEDULER_INVOCATIONS");
            long previousCount = invocationCounts.put(scheduleName, currentCount);
            if (scheduleName.equals("TestActionScheduler")) {
                assertTrue("Count decreased for " + scheduleName, previousCount < currentCount);
            } else {
                assertTrue("Count increased for " + scheduleName + " from " + previousCount + " to " + currentCount,
                        previousCount > currentCount);
            }
        }

        Thread.sleep(5);

        // Update class dep so all should restart
        vc = new VoltCompiler(false);
        jarFile = new InMemoryJarfile();
        vc.addClassToJar(jarFile, TestTaskManager.class);
        jarFile.removeClassFromJar(TestActionSchedulerParams.class.getName());
        processUpdateSync(jarFile.getLoader(), true, task1, task2);
        Thread.sleep(5);

        table = getScheduleStats();
        while (table.advanceRow()) {
            String scheduleName = table.getString("TASK_NAME");
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
        Task task = createTask(TestActionScheduler.class, TaskScope.PARTITIONS);
        m_procedure.setSinglepartition(true);
        task.setOnerror("IGNORE");

        startSync(task);
        promotedPartitionsSync(0, 1, 2, 3, 4, 5);

        // Long sleep because it sometimes takes a while for the first execution
        Thread.sleep(50);
        assertEquals(6, s_firstActionSchedulerCallCount.get());
        assertTrue("ActionScheduler should have been called at least once: " + s_postRunActionSchedulerCallCount.get(),
                s_postRunActionSchedulerCallCount.get() > 0);

        validateStats(6, "RUNNING", r -> assertNull(r.getString("SCHEDULER_STATUS")));

        task.setOnerror("ABORT");
        processUpdateSync(task);
        Thread.sleep(5);

        validateStats(6, "ERROR", r -> assertTrue(r.getString("SCHEDULER_STATUS").startsWith("Procedure ")));
    }

    /*
     * Test that the validate parameters method is being called correctly
     */
    @Test
    public void testValidateParameters() throws Exception {
        Task task = createTask(TestActionSchedulerValidateParams.class, TaskScope.HOSTS, new Object[1]);

        assertTrue(validateTask(task).isValid());

        task.setSchedulerclass(TestActionSchedulerValidateParamsWithHelper.class.getName());
        assertTrue(validateTask(task).isValid());

        // Parameter fails validation
        task.getSchedulerparameters().get("0").setParameter("FAIL");
        assertFalse(validateTask(task).isValid());

        task.setSchedulerclass(TestActionSchedulerValidateParams.class.getName());
        assertFalse(validateTask(task).isValid());
    }

    /*
     * Test providing a custom ActionSchedule to the manager
     */
    @Test
    public void testCustomSchedule() throws Exception {
        Task task = createTask(TestActionSchedule.class, TaskScope.DATABASE, 50, 250);

        startSync();
        promoteToLeaderSync(task);

        Thread.sleep(50);
        assertEquals(1, s_firstActionSchedulerCallCount.get());
        assertTrue("ActionSchedule should have been called at least once: " + s_postRunActionSchedulerCallCount.get(),
                s_postRunActionSchedulerCallCount.get() > 0);

        // SingleProcGenerator does a procedure lookup so include it
        dropScheduleAndAssertCounts(1, 1);
    }

    /*
     * Test providing a custom ActionGenerator to the manager
     */
    @Test
    public void testCustomGenerator() throws Exception {
        Task task = createTask(TestActionGenerator.class, TaskScope.DATABASE);

        startSync();
        promoteToLeaderSync(task);

        Thread.sleep(50);
        assertEquals(1, s_firstActionSchedulerCallCount.get());
        assertTrue("ActionSchedule should have been called at least once: " + s_postRunActionSchedulerCallCount.get(),
                s_postRunActionSchedulerCallCount.get() > 0);
        validateStats(1);
        processUpdateSync();

        // Same as assertCountsAfterScheduleCanceled(1) except only 1/2 the calls are to the procedure
        int previousCount = s_postRunActionSchedulerCallCount.get();
        Thread.sleep(10);
        assertEquals(1, s_firstActionSchedulerCallCount.get());
        assertEquals(previousCount, s_postRunActionSchedulerCallCount.get());

        int procedureCalls = previousCount/2;

        verify(m_internalConnectionHandler, atLeast(procedureCalls)).callProcedure(any(), any(), eq(false), any(),
                eq(m_procedure), any(), eq(false), any());
        verify(m_internalConnectionHandler, atMost(procedureCalls + 1)).callProcedure(any(), any(), eq(false), any(),
                eq(m_procedure), any(), eq(false), any());

        verify(m_clientInterface, atLeast(procedureCalls)).getProcedureFromName(eq(PROCEDURE_NAME));
        verify(m_clientInterface, atMost(procedureCalls + 1)).getProcedureFromName(eq(PROCEDURE_NAME));
    }

    /*
     * Test that validation of username works
     */
    @Test
    public void testInvalidUser() throws Exception {
        Task task = createTask(TestActionScheduler.class, TaskScope.DATABASE);

        // Create user to test
        m_database.getUsers().add(m_name.getMethodName());

        // Test user created
        task.setUser(m_name.getMethodName());
        assertTrue(validateTask(task).isValid());

        // Test invalid user
        task.setUser("fakeUser");
        assertFalse(validateTask(task).isValid());
    }

    /*
     * Test stats when a manager starts with disabled tasks
     */
    @Test
    public void disabledTasks() throws Exception {
        Task task1 = createTask(TestActionScheduler.class, TaskScope.DATABASE);
        Task task2 = createTask(TestActionScheduler.class, TaskScope.PARTITIONS);

        // Start manager with all tasks disabled
        task1.setEnabled(false);
        task2.setEnabled(false);

        startSync(task1, task2);
        validateStats(0);

        promoteToLeaderSync(task1, task2);
        validateStats(1, "DISABLED", null);

        promotedPartitionsSync(0, 1, 2, 3);
        validateStats(5, "DISABLED", null);

        // Enable tasks
        task1.setEnabled(true);
        task2.setEnabled(true);
        processUpdateSync(task1, task2);
        validateStats(5);
    }

    @Test
    public void mustExecuteWorkProcsOnPartitions() throws Exception {
        Task task = createTask(TestActionSchedule.class, TaskScope.DATABASE, 10, 100);
        m_procedure.setSinglepartition(true);
        m_procedure.setPartitionparameter(-1);

        assertFalse(validateTask(task).isValid());

        task.setScope(TaskScope.HOSTS.getId());
        assertFalse(validateTask(task).isValid());

        task.setScope(TaskScope.PARTITIONS.getId());
        assertTrue(validateTask(task).isValid());
    }

    /*
     * Test that during elastic join partition tasks are not started until after the partition is actually ready
     */
    @Test
    public void delayPartitionStartDuringJoin() throws Exception {
        m_taskManager.shutdown();
        m_taskManager = new TaskManager(m_clientInterface, m_statsAgent, 0, true, () -> m_readOnly);

        Task task = createTask(TestActionSchedule.class, TaskScope.PARTITIONS, 10, 100);
        m_procedure.setSinglepartition(true);
        m_procedure.setPartitionparameter(-1);

        startSync(task);
        assertEquals(0, s_firstActionSchedulerCallCount.get());

        promotedPartitionsSync(0, 1, 2, 3);
        assertEquals(0, s_firstActionSchedulerCallCount.get());

        enableTasksOnPartitionsSync();
        Thread.sleep(50);
        assertEquals(4, s_firstActionSchedulerCallCount.get());

        promotedPartitionsSync(4);
        Thread.sleep(5);
        assertEquals(5, s_firstActionSchedulerCallCount.get());

        validateStats(5);
    }

    /*
     * Test that tasks which are not read only are PAUSED in read only mode
     */
    @Test
    public void readOnlyMode() throws Exception {
        when(m_response.getStatus())
                .then(m -> m_readOnly ? ClientResponse.SERVER_UNAVAILABLE : ClientResponse.SUCCESS);
        m_readOnly = true;

        Task task1 = createTask(TestActionScheduler.class, TaskScope.DATABASE);
        Task task2 = createTask(TestReadOnlyScheduler.class, TaskScope.DATABASE);

        startSync(task1, task2);
        assertEquals(0, s_firstActionSchedulerCallCount.get());

        promoteToLeaderSync(task1, task2);
        Thread.sleep(50);
        assertEquals(1, s_firstActionSchedulerCallCount.get());

        validateStats(2, null, r -> {
            String name = r.getString("TASK_NAME");
            String state = r.getString("STATE");

            if (task1.getName().equalsIgnoreCase(name)) {
                assertEquals("PAUSED", state);
            } else if (task2.getName().equalsIgnoreCase(name)) {
                assertEquals("RUNNING", state);
            } else {
                fail("Unknown task: " + name);
            }
        });

        // DISABLE task should convert state to DISABLED
        task1.setEnabled(false);
        processUpdateSync(getClass().getClassLoader(), false, task1, task2);
        Thread.sleep(5);
        assertEquals(1, s_firstActionSchedulerCallCount.get());

        validateStats(2, null, r -> {
            String name = r.getString("TASK_NAME");
            String state = r.getString("STATE");

            if (task1.getName().equalsIgnoreCase(name)) {
                assertEquals("DISABLED", state);
            } else if (task2.getName().equalsIgnoreCase(name)) {
                assertEquals("RUNNING", state);
            } else {
                fail("Unknown task: " + name);
            }
        });

        // ENABLE should have state go back to PAUSED
        task1.setEnabled(true);
        processUpdateSync(getClass().getClassLoader(), false, task1, task2);
        Thread.sleep(5);
        assertEquals(1, s_firstActionSchedulerCallCount.get());

        validateStats(2, null, r -> {
            String name = r.getString("TASK_NAME");
            String state = r.getString("STATE");

            if (task1.getName().equalsIgnoreCase(name)) {
                assertEquals("PAUSED", state);
            } else if (task2.getName().equalsIgnoreCase(name)) {
                assertEquals("RUNNING", state);
            } else {
                fail("Unknown task: " + name);
            }
        });

        // Disable the second task and then come out of R/O mode and make sure it stays disabled
        task2.setEnabled(false);
        processUpdateSync(task1, task2);
        Thread.sleep(5);
        assertEquals(1, s_firstActionSchedulerCallCount.get());

        validateStats(2, null, r -> {
            String name = r.getString("TASK_NAME");
            String state = r.getString("STATE");

            if (task1.getName().equalsIgnoreCase(name)) {
                assertEquals("PAUSED", state);
            } else if (task2.getName().equalsIgnoreCase(name)) {
                assertEquals("DISABLED", state);
            } else {
                fail("Unknown task: " + name);
            }
        });

        // Make the system read/write and everything should run
        m_readOnly = false;
        m_taskManager.evaluateReadOnlyMode().get();
        Thread.sleep(5);
        assertEquals(2, s_firstActionSchedulerCallCount.get());

        validateStats(2, null, r -> {
            String name = r.getString("TASK_NAME");
            String state = r.getString("STATE");

            if (task1.getName().equalsIgnoreCase(name)) {
                assertEquals("RUNNING", state);
            } else if (task2.getName().equalsIgnoreCase(name)) {
                assertEquals("DISABLED", state);
            } else {
                fail("Unknown task: " + name);
            }
        });

        task2.setEnabled(true);
        processUpdateSync(task1, task2);
        Thread.sleep(5);
        assertEquals(3, s_firstActionSchedulerCallCount.get());
        validateStats(2);

        // Go back to read only mode
        m_readOnly = true;

        // Let the tasks run a bit before calling evaluateReadOnlyMode so they should see server unavailable errors
        Thread.sleep(5);
        validateStats(2);

        m_taskManager.evaluateReadOnlyMode().get();
        Thread.sleep(5);
        validateStats(2, null, r -> {
            String name = r.getString("TASK_NAME");
            String state = r.getString("STATE");

            if (task1.getName().equalsIgnoreCase(name)) {
                assertEquals("PAUSED", state);
            } else if (task2.getName().equalsIgnoreCase(name)) {
                assertEquals("RUNNING", state);
            } else {
                fail("Unknown task: " + name);
            }
        });
    }

    private void dropScheduleAndAssertCounts() throws Exception {
        dropScheduleAndAssertCounts(1);
    }

    private void dropScheduleAndAssertCounts(int startCount) throws Exception {
        dropScheduleAndAssertCounts(startCount, 0);
    }

    private void dropScheduleAndAssertCounts(int startCount, int extraProcLookups) throws Exception {
        validateStats(1);
        processUpdateSync();
        assertCountsAfterScheduleCanceled(startCount, extraProcLookups);
    }

    private void assertCountsAfterScheduleCanceled(int startCount, int extraProcLookups)
            throws InterruptedException {
        int previousCount = s_postRunActionSchedulerCallCount.get();
        Thread.sleep(10);
        assertEquals(startCount, s_firstActionSchedulerCallCount.get());
        assertEquals(previousCount, s_postRunActionSchedulerCallCount.get());

        verify(m_internalConnectionHandler, atLeast(previousCount)).callProcedure(any(), any(), eq(false), any(),
                eq(m_procedure), any(), eq(false), any());
        verify(m_internalConnectionHandler, atMost(previousCount + startCount)).callProcedure(any(), any(), eq(false),
                any(), eq(m_procedure), any(), eq(false), any());

        previousCount += extraProcLookups * startCount;
        verify(m_clientInterface, atLeast(previousCount)).getProcedureFromName(eq(PROCEDURE_NAME));
        verify(m_clientInterface, atMost(previousCount + startCount)).getProcedureFromName(eq(PROCEDURE_NAME));
    }

    private Task createTask(Class<? extends Initializable> clazz, TaskScope scope, Object... params) {
        return createTask(m_name.getMethodName() + m_taskNumber++, clazz, scope, params);
    }

    private Task createTask(String name, Class<? extends Initializable> clazz, TaskScope scope, Object... params) {
        Task task = initializeTask(name, scope);

        if (ActionScheduler.class.isAssignableFrom(clazz)) {
            task.setSchedulerclass(clazz.getName());
            setParameters(task.getSchedulerparameters(), params);
        } else if (ActionGenerator.class.isAssignableFrom(clazz)) {
            task.setActiongeneratorclass(clazz.getName());
            setParameters(task.getActiongeneratorparameters(), params);

            task.setScheduleclass(DelaySchedule.class.getName());
            setParameters(task.getScheduleparameters(), 1, "MILLISECONDS");
        } else if (ActionSchedule.class.isAssignableFrom(clazz)) {
            task.setScheduleclass(clazz.getName());
            setParameters(task.getScheduleparameters(), params);

            task.setActiongeneratorclass(SingleProcGenerator.class.getName());
            setParameters(task.getActiongeneratorparameters(), PROCEDURE_NAME);
        } else {
            fail("Unsupported class: " + clazz);
        }

        return task;
    }

    private void setParameters(CatalogMap<TaskParameter> paramMap, Object... params) {
        for (int i = 0; i < params.length; ++i) {
            TaskParameter tp = paramMap.add(Integer.toString(i));
            tp.setIndex(i);
            tp.setParameter(params[i] == null ? null : params[i].toString());
        }
    }

    private Task initializeTask(String name, TaskScope scope) {
        Task task = m_database.getTasks().add(name);
        task.setEnabled(true);
        task.setName(name);
        task.setScope(scope.getId());
        task.setUser(USER_NAME);
        task.setOnerror("STOP");
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
        m_taskManager.processUpdate(m_schedulesConfig, Arrays.asList(tasks), m_authSystem, classLoader, classesUpdated)
                .get();
    }

    private void promotedPartitionsSync(int... partitionIds) throws InterruptedException, ExecutionException {
        List<ListenableFuture<?>> futures = new ArrayList<>(partitionIds.length);
        for (int partitionId : partitionIds) {
            futures.add(m_taskManager.promotedPartition(partitionId));
        }
        Futures.whenAllSucceed(futures).call(() -> null).get();
    }

    private void enableTasksOnPartitionsSync() throws InterruptedException, ExecutionException {
        m_taskManager.enableTasksOnPartitions().get();
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

    private void validateStats(int statsCount) {
        validateStats(statsCount, "RUNNING", null);
    }

    private void validateStats(int statsCount, String state, Consumer<VoltTableRow> validator) {
        VoltTable table = getScheduleStats();
        long totalActionSchedulerInvocations = 0;
        long totalProcedureInvocations = 0;
        assertEquals(statsCount, table.getRowCount());
        while (table.advanceRow()) {
            if (validator != null) {
                validator.accept(table);
            }
            if (state != null) {
                assertEquals(state, table.getString("STATE"));
            }
            totalActionSchedulerInvocations += table.getLong("SCHEDULER_INVOCATIONS");
            totalProcedureInvocations += table.getLong("PROCEDURE_INVOCATIONS");
        }

        assertTrue(totalActionSchedulerInvocations >= totalProcedureInvocations);
        assertTrue(totalActionSchedulerInvocations <= s_firstActionSchedulerCallCount.get()
                + s_postRunActionSchedulerCallCount.get());
    }

    private TaskValidationResult validateTask(Task task) {
        return TaskManager.validateTask(task, TaskScope.fromId(task.getScope()), m_database, getClass().getClassLoader());
    }

    public static class TestActionScheduler implements ActionScheduler {
        @Override
        public DelayedAction getFirstDelayedAction() {
            s_firstActionSchedulerCallCount.getAndIncrement();
            return DelayedAction.createProcedure(100, TimeUnit.MICROSECONDS, this::getNextAction, PROCEDURE_NAME);
        }

        public DelayedAction getNextAction(ActionResult previousProcedureRun) {
            s_postRunActionSchedulerCallCount.getAndIncrement();
            return DelayedAction.createProcedure(100, TimeUnit.MICROSECONDS, this::getNextAction, PROCEDURE_NAME);
        }

        @Override
        public Collection<String> getDependencies() {
            return Collections.singleton(TestActionSchedulerParams.class.getName());
        }
    }

    public static class TestActionSchedulerParams implements ActionScheduler {
        public void initialize(int arg1, String arg2, byte[] arg3) {}

        @Override
        public DelayedAction getFirstDelayedAction() {
            s_firstActionSchedulerCallCount.getAndIncrement();
            return DelayedAction.createProcedure(100, TimeUnit.MICROSECONDS, this::getNextAction, PROCEDURE_NAME);
        }

        public DelayedAction getNextAction(ActionResult previousProcedureRun) {
            s_postRunActionSchedulerCallCount.getAndIncrement();
            return DelayedAction.createProcedure(100, TimeUnit.MICROSECONDS, this::getNextAction, PROCEDURE_NAME);
        }
    }

    public static class TestActionSchedulerRerun implements ActionScheduler {
        private int m_maxRunCount;
        private int m_runCount = 0;

        public void initialize(int maxRunCount) {
            m_maxRunCount = maxRunCount;
        }

        @Override
        public DelayedAction getFirstDelayedAction() {
            s_firstActionSchedulerCallCount.getAndIncrement();
            return ++m_runCount < m_maxRunCount
                    ? DelayedAction.createCallback(100, TimeUnit.MICROSECONDS, this::getNextAction)
                    : DelayedAction.createExit(null);
        }

        public DelayedAction getNextAction(ActionResult previousProcedureRun) {
            assertNull(previousProcedureRun.getProcedure());
            s_postRunActionSchedulerCallCount.getAndIncrement();

            return ++m_runCount < m_maxRunCount
                    ? DelayedAction.createCallback(100, TimeUnit.MICROSECONDS, this::getNextAction)
                    : DelayedAction.createExit(null);
        }
    }

    public static class TestActionSchedulerValidateParams implements ActionScheduler {
        public static String validateParameters(String value) {
            return value;
        }

        public void initialize(String value) {}

        @Override
        public DelayedAction getFirstDelayedAction() {
            return null;
        }
    }

    public static class TestActionSchedulerValidateParamsWithHelper extends TestActionSchedulerValidateParams {
        public static String validateParameters(TaskHelper helper, String value) {
            return TestActionSchedulerValidateParams.validateParameters(value);
        }

        public void initialize(TaskHelper helper, String value) {
            super.initialize(value);
        }
    }

    public static class TestActionSchedule implements ActionSchedule {
        private long m_min;
        private long m_max;

        public void initialize(long min, long max) {
            m_min = min;
            m_max = max;
        }

        @Override
        public ActionDelay getFirstDelay() {
            s_firstActionSchedulerCallCount.getAndIncrement();
            return new ActionDelay(ThreadLocalRandom.current().nextLong(m_min, m_max), TimeUnit.NANOSECONDS,
                    this::getNextDelay);
        }

        private ActionDelay getNextDelay(ActionResult result) {
            s_postRunActionSchedulerCallCount.getAndIncrement();
            return new ActionDelay(ThreadLocalRandom.current().nextLong(m_min, m_max), TimeUnit.NANOSECONDS,
                    this::getNextDelay);
        }
    }

    public static class TestActionGenerator implements ActionGenerator {
        private long m_count = 0;

        @Override
        public Action getFirstAction() {
            s_firstActionSchedulerCallCount.getAndIncrement();
            return Action.createProcedure(this::getNextAction, PROCEDURE_NAME);
        }

        private Action getNextAction(ActionResult result) {
            s_postRunActionSchedulerCallCount.getAndIncrement();
            return (++m_count & 0x1) == 0 ? Action.createProcedure(this::getNextAction, PROCEDURE_NAME)
                    : Action.createCallback(this::getNextAction);
        }
    }

    public static class TestReadOnlyScheduler implements ActionScheduler {
        @Override
        public DelayedAction getFirstDelayedAction() {
            s_firstActionSchedulerCallCount.getAndIncrement();
            return DelayedAction.createCallback(10, TimeUnit.MICROSECONDS, this::getNextAction);
        }

        private DelayedAction getNextAction(ActionResult result) {
            s_postRunActionSchedulerCallCount.getAndIncrement();
            return DelayedAction.createCallback(10, TimeUnit.MICROSECONDS, this::getNextAction);
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }
    }
}
