/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.voltdb.*;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.AuthSystem.InternalAdminUser;
import org.voltdb.catalog.*;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.deploymentfile.TaskSettingsType;
import org.voltdb.task.TaskManager.TaskValidationResult;
import org.voltdb.utils.InMemoryJarfile;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class TestTaskManager {

    static AtomicInteger s_firstActionSchedulerCallCount = new AtomicInteger();
    static AtomicInteger s_postRunActionSchedulerCallCount = new AtomicInteger();

    private static final String PROCEDURE_NAME = "SomeProcedure";
    private static final String USER_NAME = "user";

    // This test is timing-sensitive so use a 1s wait timeout
    private static final Duration WAIT_TIMEOUT = Durations.ONE_SECOND;

    @Rule
    public final TestName m_name = new TestName();

    private boolean m_readOnly;
    private AuthSystem m_authSystem;
    private ClientInterface m_clientInterface;
    private InternalConnectionHandler m_internalConnectionHandler;
    private TaskManager m_taskManager;
    private Database m_database;
    private Procedure m_procedure;
    private final StatsAgent m_statsAgent = new StatsAgent();
    private final TaskSettingsType m_schedulesConfig = new TaskSettingsType();
    private ClientResponse m_response;
    private int m_taskNumber = 0;

    @Before
    public void setup() {
        Awaitility.setDefaultPollInterval(Durations.ONE_MILLISECOND);
        Awaitility.setDefaultTimeout(WAIT_TIMEOUT);

        m_database = new Catalog().getClusters().add("cluster").getDatabases().add("database");
        m_database.getUsers().add(USER_NAME);
        m_procedure = m_database.getProcedures().add(PROCEDURE_NAME);

        m_authSystem = mock(AuthSystem.class);
        when(m_authSystem.getUser(anyString())).then(m -> mock(AuthUser.class));
        when(m_authSystem.getInternalAdminUser()).then(m -> mock(InternalAdminUser.class));

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

        startSync(ImmutableMap.of());
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(0));

        processUpdateSync(ImmutableMap.of(), task);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(0));

        promoteToLeaderSync(ImmutableMap.of(task.getName(), true), task);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(1));

        // ActionScheduler should have been called at least once
        await().untilAtomic(s_postRunActionSchedulerCallCount, greaterThan(0));

        dropScheduleAndAssertCounts();
    }

    /*
     * Test that a host schedule is once configured and then is stopped when the configuration is removed
     */
    @Test
    public void hostScheduleCreateDrop() throws Exception {
        Task task = createTask(TestActionScheduler.class, TaskScope.HOSTS);
        m_procedure.setTransactional(false);

        startSync(ImmutableMap.of());
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(0));

        processUpdateSync(ImmutableMap.of(task.getName(), true), task);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(1));

        // ActionScheduler should have been called at least once
        await().untilAtomic(s_postRunActionSchedulerCallCount, greaterThan(0));

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

        startSync(ImmutableMap.of());
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(0));

        processUpdateSync(ImmutableMap.of(task.getName(), true), task);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(0));

        promotedPartitionsSync(0, 4);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(2));

        // ActionScheduler should have been called at least once
        await().untilAtomic(s_postRunActionSchedulerCallCount, greaterThan(0));

        demotedPartitionsSync(0, 4);
        assertCountsAfterScheduleCanceled(2, 0);

        int previousCount = s_postRunActionSchedulerCallCount.get();
        promotedPartitionsSync(0);

        // ActionScheduler should have been called at least previousCount times
        await().untilAtomic(s_postRunActionSchedulerCallCount, greaterThan(previousCount));

        dropScheduleAndAssertCounts(3);
    }

    /*
     * Test that a scheduler which takes paramaters can be constructed and executed
     */
    @Test
    public void schedulerWithParameters() throws Exception {
        Task task = createTask(TestActionSchedulerParams.class, TaskScope.DATABASE, 5, "TESTING", "AFFA47");

        startSync(ImmutableMap.of());
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(0));

        processUpdateSync(ImmutableMap.of(), task);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(0));

        promoteToLeaderSync(ImmutableMap.of(task.getName(), true), task);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(1));

        // ActionScheduler should have been called at least once
        await().untilAtomic(s_postRunActionSchedulerCallCount, greaterThan(0));

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

        startSync(ImmutableMap.of(task2.getName(), true), task1, task2);
        promoteToLeaderSync(ImmutableMap.of(task1.getName(), true), task1, task2);
        promotedPartitionsSync(0, 1, 2, 3);

        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(5));
        m_taskManager.shutdown().get();
    }

    /*
     * Test that a scheduler can just schedule itself to be rerun multple times
     */
    @Test
    public void rerunActionScheduler() throws Exception {
        Task task = createTask(TestActionSchedulerRerun.class, TaskScope.DATABASE, 5);

        startSync(ImmutableMap.of(), task);
        promoteToLeaderSync(ImmutableMap.of(task.getName(), true), task);

        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(1));
        await().untilAtomic(s_postRunActionSchedulerCallCount, equalTo(4));
    }

    /*
     * Test that stats are maintained while a previously running procedure is disabled but scheduler is restarted when
     * enabled
     */
    @Test
    public void disableReenableActionScheduler() throws Exception {
        Task task = createTask(TestActionScheduler.class, TaskScope.DATABASE);

        startSync(ImmutableMap.of());
        promoteToLeaderSync(ImmutableMap.of(task.getName(), true), task);

        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(1));

        task.setEnabled(false);
        processUpdateSync(ImmutableMap.of(), task);
        checkStats(1, "DISABLED", null);

        task.setEnabled(true);
        processUpdateSync(ImmutableMap.of(), task);
        dropScheduleAndAssertCounts(2);
    }

    /*
     * Test that newly promoted partitions honor the enable flag of a procedure
     */
    @Test
    public void partitionPromotionAndDisabledSchedules() throws Exception {
        Task task = createTask(TestActionSchedulerRerun.class, TaskScope.PARTITIONS, 5);

        startSync(ImmutableMap.of(task.getName(), true), task);

        task.setEnabled(false);
        processUpdateSync(ImmutableMap.of(), task);

        promotedPartitionsSync(0, 1);

        await().until(() -> getScheduleStats().getRowCount(), equalTo(2));

        task.setEnabled(true);
        processUpdateSync(ImmutableMap.of(), task);

        await().until(() -> getScheduleStats().getRowCount(), equalTo(2));

        promotedPartitionsSync(2, 3);
        await().until(() -> getScheduleStats().getRowCount(), equalTo(4));

        task.setEnabled(false);
        processUpdateSync(ImmutableMap.of(), task);

        promotedPartitionsSync(4, 5);
        await().until(() -> getScheduleStats().getRowCount(), equalTo(6));

        task.setEnabled(true);
        processUpdateSync(ImmutableMap.of(), task);

        await().until(() -> getScheduleStats().getRowCount(), equalTo(6));

        task.setEnabled(false);
        processUpdateSync(ImmutableMap.of(), task);

        demotedPartitionsSync(3, 4, 5);
        await().until(() -> getScheduleStats().getRowCount(), equalTo(3));
    }

    /*
     * Test that minimum delay configuration is honored
     */
    @Test
    public void minDelay() throws Exception {
        m_schedulesConfig.setMininterval(10000);
        Task task = createTask(TestActionScheduler.class, TaskScope.DATABASE);
        startSync(ImmutableMap.of());
        promoteToLeaderSync(ImmutableMap.of(task.getName(), true), task);

        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(1));
        await().untilAtomic(s_postRunActionSchedulerCallCount, equalTo(0));
    }

    /*
     * Test that max run frequency configuration is honored
     */
    @Test(timeout = 1_000)
    public void maxRunFrequency() throws Exception {
        m_schedulesConfig.setMaxfrequency(1.0);
        Task task = createTask(TestActionScheduler.class, TaskScope.DATABASE);
        startSync(ImmutableMap.of());
        promoteToLeaderSync(ImmutableMap.of(task.getName(), true), task);

        await().untilAsserted(() -> {
            assertEquals(1, s_firstActionSchedulerCallCount.get());
            assertEquals(1, s_postRunActionSchedulerCallCount.get());
        });
    }

    /*
     * Test that schedules are only restarted when classes are updated and only if the class or one of its deps change
     *
     * TODO test changing the class restarts the schedule
     */
    @Test
    public void reloadWithInMemoryJarFile() throws Exception {
        InMemoryJarfile jarFile = new InMemoryJarfile();
        VoltCompiler vc = new VoltCompiler(false);
        vc.addClassToJar(jarFile, TestTaskManager.class);

        Task task1 = createTask("TestActionScheduler", TestActionScheduler.class, TaskScope.DATABASE);
        Task task2 = createTask("TestActionSchedulerRerun", TestActionSchedulerRerun.class, TaskScope.DATABASE,
                Integer.MAX_VALUE);

        startSync(ImmutableMap.of());
        promoteToLeaderSync(ImmutableMap.of());
        processUpdateSync(ImmutableMap.of(task1.getName(), true, task2.getName(), true), jarFile.getLoader(), false,
                task1, task2);

        Map<String, Long> invocationCounts = new HashMap<>();
        VoltTable voltTable = getScheduleStats();
        while (voltTable.advanceRow()) {
            invocationCounts.put(voltTable.getString("TASK_NAME"), voltTable.getLong("SCHEDULER_INVOCATIONS"));
        }

        // No schedules should restart since class and deps did not change
        processUpdateSync(ImmutableMap.of(), jarFile.getLoader(), false,
                task1, task2);

        await().untilAsserted(() -> {
            VoltTable table = getScheduleStats();
            while (table.advanceRow()) {
                String scheduleName = table.getString("TASK_NAME");
                long currentCount = table.getLong("SCHEDULER_INVOCATIONS");
                assertTrue("Count decreased for " + scheduleName,
                        invocationCounts.put(scheduleName, currentCount) < currentCount);
            }
        });

        // Only schedules which do not specify deps should restart
        processUpdateSync(ImmutableMap.of(task2.getName(), true), jarFile.getLoader(), true,
                task1, task2);

        await().untilAsserted(() -> {
            VoltTable table = getScheduleStats();
            while (table.advanceRow()) {
                String scheduleName = table.getString("TASK_NAME");
                long currentCount = table.getLong("SCHEDULER_INVOCATIONS");
                long previousCount = invocationCounts.put(scheduleName, currentCount);
                if (scheduleName.equals("TestActionScheduler")) {
                    assertTrue("Count decreased for " + scheduleName, previousCount < currentCount);
                } else {
                    assertTrue("Count should be greater than 0: " + scheduleName, currentCount > 0);
                }
            }
        });

        // Update class dep so all should restart
        vc = new VoltCompiler(false);
        jarFile = new InMemoryJarfile();
        vc.addClassToJar(jarFile, TestTaskManager.class);
        jarFile.removeClassFromJar(TestActionSchedulerParams.class.getName());
        processUpdateSync(ImmutableMap.of(task1.getName(), true, task2.getName(), true), jarFile.getLoader(), true,
                task1, task2);

        await().untilAsserted(() -> {
            VoltTable table = getScheduleStats();
            while (table.advanceRow()) {
                String scheduleName = table.getString("TASK_NAME");
                long currentCount = table.getLong("SCHEDULER_INVOCATIONS");
                assertTrue("Count should be greater than 0: " + scheduleName, currentCount > 0);
            }
        });

        // Update implicit sub class dep so all should restart
        vc = new VoltCompiler(false);
        jarFile = new InMemoryJarfile();
        vc.addClassToJar(jarFile, TestTaskManager.class);
        jarFile.removeClassFromJar(TestActionSchedulerParams.class.getName());
        jarFile.removeClassFromJar(TestActionScheduler.Dummy.class.getName());
        processUpdateSync(ImmutableMap.of(task1.getName(), true, task2.getName(), true), jarFile.getLoader(), true,
                task1, task2);

        await().untilAsserted(() -> {
            VoltTable table = getScheduleStats();
            while (table.advanceRow()) {
                String scheduleName = table.getString("TASK_NAME");
                long currentCount = table.getLong("SCHEDULER_INVOCATIONS");
                assertTrue("Count should be greater than 0: " + scheduleName, currentCount > 0);
            }
        });
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

        startSync(ImmutableMap.of(task.getName(), true), task);
        promotedPartitionsSync(0, 1, 2, 3, 4, 5);

        // Long sleep because it sometimes takes a while for the first execution
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(6));
        await().untilAtomic(s_postRunActionSchedulerCallCount, greaterThan(0));

        checkStats(6, "RUNNING", r -> null == r.getString("SCHEDULER_STATUS"));

        task.setOnerror("ABORT");
        processUpdateSync(ImmutableMap.of(), task);

        checkStats(6, "ERROR", r -> {
            String status = r.getString("SCHEDULER_STATUS");
            return status != null && status.startsWith("Procedure ");
        });
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
        Task task = createTask(TestIntervalGenerator.class, TaskScope.DATABASE, 50, 250);

        startSync(ImmutableMap.of());
        promoteToLeaderSync(ImmutableMap.of(task.getName(), true), task);

        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(1));
        await().untilAtomic(s_postRunActionSchedulerCallCount, greaterThan(0));

        // SingleProcGenerator does a procedure lookup so include it
        dropScheduleAndAssertCounts(1, 1);
    }

    /*
     * Test providing a custom ActionGenerator to the manager
     */
    @Test
    public void testCustomGenerator() throws Exception {
        Task task = createTask(TestActionGenerator.class, TaskScope.DATABASE);

        startSync(ImmutableMap.of());
        promoteToLeaderSync(ImmutableMap.of(task.getName(), true), task);

        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(1));
        await().untilAtomic(s_postRunActionSchedulerCallCount, greaterThan(0));

        checkStats(1);
        processUpdateSync(ImmutableMap.of(task.getName(), false));

        // Same as assertCountsAfterScheduleCanceled(1) except only 1/2 the calls are to the procedure
        int previousCount = s_postRunActionSchedulerCallCount.get();
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(1));
        await().untilAtomic(s_postRunActionSchedulerCallCount, equalTo(previousCount));

        int procedureCalls = previousCount / 2;

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

        startSync(ImmutableMap.of(task2.getName(), true), task1, task2);
        checkStats(0);

        promoteToLeaderSync(ImmutableMap.of(task1.getName(), true), task1, task2);
        checkStats(1, "DISABLED", null);

        promotedPartitionsSync(0, 1, 2, 3);
        checkStats(5, "DISABLED", null);

        // Enable tasks
        task1.setEnabled(true);
        task2.setEnabled(true);
        processUpdateSync(ImmutableMap.of(), task1, task2);
        checkStats(5);
    }

    @Test
    public void mustExecuteWorkProcsOnPartitions() throws Exception {
        Task task = createTask(TestIntervalGenerator.class, TaskScope.DATABASE, 10, 100);
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

        Task task = createTask(TestIntervalGenerator.class, TaskScope.PARTITIONS, 10, 100);
        m_procedure.setSinglepartition(true);
        m_procedure.setPartitionparameter(-1);

        startSync(ImmutableMap.of(task.getName(), true), task);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(0));

        promotedPartitionsSync(0, 1, 2, 3);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(0));

        enableTasksOnPartitionsSync();
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(4));

        promotedPartitionsSync(4);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(5));

        checkStats(5);
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

        startSync(ImmutableMap.of(), task1, task2);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(0));

        promoteToLeaderSync(ImmutableMap.of(task1.getName(), true, task2.getName(), true), task1, task2);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(1));

        checkStats(2, null, r -> {
            String name = r.getString("TASK_NAME");
            String state = r.getString("STATE");

            if (task1.getName().equalsIgnoreCase(name)) {
                return state.equals("PAUSED");
            } else if (task2.getName().equalsIgnoreCase(name)) {
                return state.equals("RUNNING");
            } else {
                fail("Unknown task: " + name);
            }
            return true;
        });

        // DISABLE task should convert state to DISABLED
        task1.setEnabled(false);
        processUpdateSync(ImmutableMap.of(), getClass().getClassLoader(),
                false, task1, task2);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(1));

        checkStats(2, null, r -> {
            String name = r.getString("TASK_NAME");
            String state = r.getString("STATE");

            if (task1.getName().equalsIgnoreCase(name)) {
                return state.equals("DISABLED");
            } else if (task2.getName().equalsIgnoreCase(name)) {
                return state.equals("RUNNING");
            } else {
                fail("Unknown task: " + name);
            }
            return true;
        });

        // ENABLE should have state go back to PAUSED
        task1.setEnabled(true);
        processUpdateSync(ImmutableMap.of(), getClass().getClassLoader(),
                false, task1, task2);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(1));

        checkStats(2, null, r -> {
            String name = r.getString("TASK_NAME");
            String state = r.getString("STATE");

            if (task1.getName().equalsIgnoreCase(name)) {
                return state.equals("PAUSED");
            } else if (task2.getName().equalsIgnoreCase(name)) {
                return state.equals("RUNNING");
            } else {
                fail("Unknown task: " + name);
            }
            return true;
        });


        // Disable the second task and then come out of R/O mode and make sure it stays disabled
        task2.setEnabled(false);
        processUpdateSync(ImmutableMap.of(), task1, task2);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(1));

        checkStats(2, null, r -> {
            String name = r.getString("TASK_NAME");
            String state = r.getString("STATE");

            if (task1.getName().equalsIgnoreCase(name)) {
                return state.equals("PAUSED");
            } else if (task2.getName().equalsIgnoreCase(name)) {
                return state.equals("DISABLED");
            } else {
                fail("Unknown task: " + name);
            }
            return true;
        });

        // Make the system read/write and everything should run
        m_readOnly = false;
        m_taskManager.evaluateReadOnlyMode().get();
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(2));

        checkStats(2, null, r -> {
            String name = r.getString("TASK_NAME");
            String state = r.getString("STATE");

            if (task1.getName().equalsIgnoreCase(name)) {
                return state.equals("RUNNING");
            } else if (task2.getName().equalsIgnoreCase(name)) {
                return state.equals("DISABLED");
            } else {
                fail("Unknown task: " + name);
            }
            return true;
        });

        task2.setEnabled(true);
        processUpdateSync(ImmutableMap.of(), task1, task2);
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(3));
        checkStats(2);

        // Go back to read only mode
        m_readOnly = true;

        // Let the tasks run a bit before calling evaluateReadOnlyMode so they should see server unavailable errors
        checkStats(2);

        m_taskManager.evaluateReadOnlyMode().get();
        checkStats(2, null, r -> {
            String name = r.getString("TASK_NAME");
            String state = r.getString("STATE");

            if (task1.getName().equalsIgnoreCase(name)) {
                return state.equals("PAUSED");
            } else if (task2.getName().equalsIgnoreCase(name)) {
                return state.equals("RUNNING");
            } else {
                fail("Unknown task: " + name);
            }

            return true;
        });
    }

    /*
     * Test that system tasks can be created and are not affected by catalog updates
     */
    @Test
    public void systemTask() throws Exception {
        m_taskManager.addSystemTask(m_name.getMethodName(), TaskScope.PARTITIONS, h -> new TestActionScheduler()).get();
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(0));

        startSync(ImmutableMap.of());
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(0));

        promotedPartitionsSync(0, 1, 2, 3);

        // Long sleep because it sometimes takes a while for the first execution
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(4));
        await().untilAtomic(s_postRunActionSchedulerCallCount, greaterThan(0));

        // Update of no tasks does not affect system tasks
        int postRunActionSchedulerCallCount = s_postRunActionSchedulerCallCount.get();
        processUpdateSync(ImmutableMap.of());
        await().untilAtomic(s_postRunActionSchedulerCallCount, greaterThan(postRunActionSchedulerCallCount));

        assertTrue(m_taskManager.removeSystemTask(m_name.getMethodName()).get());
        assertCountsAfterScheduleCanceled(4, 0);

        assertFalse(m_taskManager.removeSystemTask(m_name.getMethodName()).get());
    }

    private void dropScheduleAndAssertCounts() throws Exception {
        dropScheduleAndAssertCounts(1);
    }

    private void dropScheduleAndAssertCounts(int startCount) throws Exception {
        dropScheduleAndAssertCounts(startCount, 0);
    }

    private void dropScheduleAndAssertCounts(int startCount, int extraProcLookups) throws Exception {
        checkStats(1);
        for (Boolean modification : m_taskManager.processUpdate(m_schedulesConfig, Collections.emptyList(),
                m_authSystem, getClass().getClassLoader(), false).get().values()) {
            assertFalse(modification);
        }
        assertCountsAfterScheduleCanceled(startCount, extraProcLookups);
    }

    private void assertCountsAfterScheduleCanceled(int startCount, int extraProcLookups) {
        checkStats(0);

        final int previousCount = s_postRunActionSchedulerCallCount.get();
        await().untilAtomic(s_firstActionSchedulerCallCount, equalTo(startCount));
        await().untilAtomic(s_postRunActionSchedulerCallCount, equalTo(previousCount));

        verify(m_internalConnectionHandler, atLeast(previousCount)).callProcedure(any(), any(), eq(false), any(),
                eq(m_procedure), any(), eq(false), any());
        verify(m_internalConnectionHandler, atMost(previousCount + startCount)).callProcedure(any(), any(), eq(false),
                any(), eq(m_procedure), any(), eq(false), any());

        int previousCount2 = previousCount + (extraProcLookups * startCount);
        verify(m_clientInterface, atLeast(previousCount2)).getProcedureFromName(eq(PROCEDURE_NAME));
        verify(m_clientInterface, atMost(previousCount2 + startCount)).getProcedureFromName(eq(PROCEDURE_NAME));
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

            task.setScheduleclass(DelayIntervalGenerator.class.getName());
            setParameters(task.getScheduleparameters(), 1, "MILLISECONDS");
        } else if (IntervalGenerator.class.isAssignableFrom(clazz)) {
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

    private void startSync(Map<String, Boolean> expected, Task... tasks)
            throws InterruptedException, ExecutionException {
        assertEquals(expected, m_taskManager
                .start(m_schedulesConfig, Arrays.asList(tasks), m_authSystem, getClass().getClassLoader()).get());
    }

    public void promoteToLeaderSync(Map<String, Boolean> expected, Task... tasks)
            throws InterruptedException, ExecutionException {
        assertEquals(expected, m_taskManager
                .promoteToLeader(m_schedulesConfig, Arrays.asList(tasks), m_authSystem, getClass().getClassLoader())
                .get());
    }

    private void processUpdateSync(Map<String, Boolean> expected, Task... tasks)
            throws InterruptedException, ExecutionException {
        processUpdateSync(expected, getClass().getClassLoader(), false, tasks);
    }

    private void processUpdateSync(Map<String, Boolean> expected, ClassLoader classLoader, boolean classesUpdated,
                                   Task... tasks) throws InterruptedException, ExecutionException {
        assertEquals(expected, m_taskManager
                .processUpdate(m_schedulesConfig, Arrays.asList(tasks), m_authSystem, classLoader, classesUpdated)
                .get());
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

    private void checkStats(int statsCount) {
        checkStats(statsCount, "RUNNING", null);
    }

    private void checkStats(int statsCount, String state, Function<VoltTableRow, Boolean> validator) {
        VoltTable table = await()
                .until(
                        this::getScheduleStats,
                        hasProperty("rowCount", equalTo(statsCount))
                );

        // Next Validate the rows
        await().untilAsserted(() -> {
                    long totalActionSchedulerInvocations = 0;
                    long totalProcedureInvocations = 0;

                    while (table.advanceRow()) {
                        if (validator != null) {
                            assertTrue(validator.apply(table));
                        }

                        if (state != null) {
                            assertEquals(state, table.getString("STATE"));
                        }

                        // Update counts for this row
                        totalActionSchedulerInvocations += table.getLong("SCHEDULER_INVOCATIONS");
                        totalProcedureInvocations += table.getLong("PROCEDURE_INVOCATIONS");
                    }

                    assertTrue(totalActionSchedulerInvocations >= totalProcedureInvocations);
                    assertTrue(totalActionSchedulerInvocations <= s_firstActionSchedulerCallCount.get()
                            + s_postRunActionSchedulerCallCount.get());
                }
        );
    }

    private TaskValidationResult validateTask(Task task) {
        return TaskManager.validateTask(task, TaskScope.fromId(task.getScope()), m_database,
                getClass().getClassLoader(), n -> m_database.getProcedures().get(n));
    }

    public static class TestActionScheduler implements ActionScheduler {
        @Override
        public ScheduledAction getFirstScheduledAction() {
            s_firstActionSchedulerCallCount.getAndIncrement();
            return ScheduledAction.procedureCall(100, TimeUnit.MICROSECONDS, this::getNextAction, PROCEDURE_NAME);
        }

        public ScheduledAction getNextAction(ActionResult previousProcedureRun) {
            s_postRunActionSchedulerCallCount.getAndIncrement();
            return ScheduledAction.procedureCall(100, TimeUnit.MICROSECONDS, this::getNextAction, PROCEDURE_NAME);
        }

        @Override
        public Collection<String> getDependencies() {
            return Collections.singleton(TestActionSchedulerParams.class.getName());
        }

        static class Dummy {
        }
    }

    public static class TestActionSchedulerParams implements ActionScheduler {
        public void initialize(int arg1, String arg2, byte[] arg3) {
        }

        @Override
        public ScheduledAction getFirstScheduledAction() {
            s_firstActionSchedulerCallCount.getAndIncrement();
            return ScheduledAction.procedureCall(100, TimeUnit.MICROSECONDS, this::getNextAction, PROCEDURE_NAME);
        }

        public ScheduledAction getNextAction(ActionResult previousProcedureRun) {
            s_postRunActionSchedulerCallCount.getAndIncrement();
            return ScheduledAction.procedureCall(100, TimeUnit.MICROSECONDS, this::getNextAction, PROCEDURE_NAME);
        }
    }

    public static class TestActionSchedulerRerun implements ActionScheduler {
        private int m_maxRunCount;
        private int m_runCount = 0;

        public void initialize(int maxRunCount) {
            m_maxRunCount = maxRunCount;
        }

        @Override
        public ScheduledAction getFirstScheduledAction() {
            s_firstActionSchedulerCallCount.getAndIncrement();
            return ++m_runCount < m_maxRunCount
                    ? ScheduledAction.callback(100, TimeUnit.MICROSECONDS, this::getNextAction)
                    : ScheduledAction.exit(null);
        }

        public ScheduledAction getNextAction(ActionResult previousProcedureRun) {
            assertNull(previousProcedureRun.getProcedure());
            s_postRunActionSchedulerCallCount.getAndIncrement();

            return ++m_runCount < m_maxRunCount
                    ? ScheduledAction.callback(100, TimeUnit.MICROSECONDS, this::getNextAction)
                    : ScheduledAction.exit(null);
        }
    }

    public static class TestActionSchedulerValidateParams implements ActionScheduler {
        public static String validateParameters(String value) {
            return value;
        }

        public void initialize(String value) {
        }

        @Override
        public ScheduledAction getFirstScheduledAction() {
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

    public static class TestIntervalGenerator implements IntervalGenerator {
        private long m_min;
        private long m_max;

        public void initialize(long min, long max) {
            m_min = min;
            m_max = max;
        }

        @Override
        public Interval getFirstInterval() {
            s_firstActionSchedulerCallCount.getAndIncrement();
            return new Interval(ThreadLocalRandom.current().nextLong(m_min, m_max), TimeUnit.NANOSECONDS,
                    this::getNextDelay);
        }

        private Interval getNextDelay(ActionResult result) {
            s_postRunActionSchedulerCallCount.getAndIncrement();
            return new Interval(ThreadLocalRandom.current().nextLong(m_min, m_max), TimeUnit.NANOSECONDS,
                    this::getNextDelay);
        }
    }

    public static class TestActionGenerator implements ActionGenerator {
        private long m_count = 0;

        @Override
        public Action getFirstAction() {
            s_firstActionSchedulerCallCount.getAndIncrement();
            return Action.procedureCall(this::getNextAction, PROCEDURE_NAME);
        }

        private Action getNextAction(ActionResult result) {
            s_postRunActionSchedulerCallCount.getAndIncrement();
            return (++m_count & 0x1) == 0 ? Action.procedureCall(this::getNextAction, PROCEDURE_NAME)
                    : Action.callback(this::getNextAction);
        }
    }

    public static class TestReadOnlyScheduler implements ActionScheduler {
        @Override
        public ScheduledAction getFirstScheduledAction() {
            s_firstActionSchedulerCallCount.getAndIncrement();
            return ScheduledAction.callback(10, TimeUnit.MICROSECONDS, this::getNextAction);
        }

        private ScheduledAction getNextAction(ActionResult result) {
            s_postRunActionSchedulerCallCount.getAndIncrement();
            return ScheduledAction.callback(10, TimeUnit.MICROSECONDS, this::getNextAction);
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }
    }
}
