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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Test;
import org.voltdb.LocalClustersTestBase;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.tasktest.CustomTestActionGenerator;
import org.voltdb.tasktest.CustomTestActionScheduler;
import org.voltdb.utils.MiscUtils;

public class TestTasksEnd2End extends LocalClustersTestBase {
    private static final String s_userName = "TestUser";

    private final VoltProjectBuilder m_builder = new VoltProjectBuilder();

    @Override
    public void setUp() throws Exception {
        super.setUp();

        String roleName = "TestRole";
        m_builder.addRoles(new VoltProjectBuilder.RoleInfo[] {
                new VoltProjectBuilder.RoleInfo(roleName, true, true, true, true, true, true) });
        m_builder.addUsers(new VoltProjectBuilder.UserInfo[] {
                new VoltProjectBuilder.UserInfo(s_userName, "password", new String[] { roleName }) });

        configureClustersAndClients(Collections.singletonList(new ClusterConfiguration(4, 3, 1, m_builder)), 2, 2);
    }

    @After
    public void cleanUpSchedules() {
        try {
            Client client = getClient(0);
            VoltTable table = client.callProcedure("@SystemCatalog", "TASKS").getResults()[0];
            StringBuilder sb = new StringBuilder();
            while (table.advanceRow()) {
                sb.append("DROP TASK ").append(table.getString("TASK_NAME")).append(';');
            }
            if (sb.length() != 0) {
                client.callProcedure("@AdHoc", sb.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * Test schedules running on the system.
     *
     * Create 3 schedules which insert a summary row into a table at different intervals
     */
    @Test
    public void systemSchedules() throws Exception {
        Client client = getClient(0);

        String summaryTable = getTableName(3, TableType.REPLICATED);
        client.callProcedure("@AdHoc", "CREATE TABLE " + summaryTable
                + " (date TIMESTAMP NOT NULL, id INT NOT NULL, cnt BIGINT NOT NULL, key_sum DECIMAL NOT NULL, value_sum DECIMAL NOT NULL);");

        AtomicReference<Exception> error = new AtomicReference<>();
        Thread producer = new Thread() {
            @Override
            public void run() {
                try {
                    while (true) {
                        insertRandomRows(client, 0, TableType.REPLICATED, 5);
                        Thread.sleep(1);
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    error.set(e);
                }
            };
        };
        producer.start();

        String schedule1 = getMethodName() + "_1";
        String schedule2 = getMethodName() + "_2";
        String schedule3 = getMethodName() + "_3";

        client.callProcedure("@AdHoc",
                "CREATE PROCEDURE " + getMethodName() + " AS INSERT INTO " + summaryTable
                        + " SELECT NOW, CAST(? AS INT), COUNT(*), SUM(CAST(key as DECIMAL)), SUM(CAST(value AS DECIMAL)) FROM "
                        + getTableName(0, TableType.REPLICATED) + ';');

        String summaryFormat = "CREATE TASK %s ON SCHEDULE %s PROCEDURE " + getMethodName()
                + " WITH (%d) ON ERROR IGNORE;";

        client.callProcedure("@AdHoc", String.format(summaryFormat, schedule1, "delay 50 MILLISECONDS", 1));
        client.callProcedure("@AdHoc", String.format(summaryFormat, schedule2, "CRON * * * * * *", 2));
        client.callProcedure("@AdHoc", String.format(summaryFormat, schedule3, "EVERY 75 milliseconds", 3));

        // Give everything some time to run
        Thread.sleep(1000);
        producer.interrupt();
        producer.join();

        VoltTable table = getTaskStats(client);
        assertEquals(3, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("RUNNING", table.getString("STATE"));
        }

        client.callProcedure("@AdHoc", "ALTER TASK " + schedule1 + " DISABLE; ALTER TASK " + schedule2
                + " DISABLE; ALTER TASK " + schedule3 + " DISABLE;");

        Thread.sleep(5);

        table = getTaskStats(client);
        assertEquals(3, table.getRowCount());
        while (table.advanceRow()) {
            String scheduleName = table.getString("TASK_NAME");
            int id = -1;
            if (schedule1.equalsIgnoreCase(scheduleName)) {
                id = 1;
            } else if (schedule2.equalsIgnoreCase(scheduleName)) {
                id = 2;
            } else if (schedule3.equalsIgnoreCase(scheduleName)) {
                id = 3;
            } else {
                fail("Unknown schedule " + scheduleName);
            }

            assertEquals("DISABLED", table.getString("STATE"));

            long summaryCount = client
                    .callProcedure("@AdHoc", "SELECT COUNT(*) FROM " + summaryTable + " WHERE id = " + id + ";")
                    .getResults()[0].asScalarLong();
            long procedureInvocations = table.getLong("PROCEDURE_INVOCATIONS");
            long successfulProcedureInvocations = procedureInvocations - table.getLong("PROCEDURE_FAILURES");

            /*
             * There can be one extra invocation since stats are done when the result comes back and a schedule can be
             * stopped after procedure is called but before the result comes back
             */
            assertTrue(
                    "Summary table has " + summaryCount + " rows. Invocation count is " + successfulProcedureInvocations
                            + " for " + scheduleName,
                    summaryCount == successfulProcedureInvocations
                            || summaryCount == successfulProcedureInvocations + 1);

            long schedulerInvocations = table.getLong("SCHEDULER_INVOCATIONS");
            assertTrue(
                    schedulerInvocations == procedureInvocations || schedulerInvocations == procedureInvocations + 1);
        }
    }

    /*
     * Test a schedule which runs on the partitions
     *
     * Create a simple schedule which prunes each partition down to 10 entries
     *
     * Test that partition failover works correctly
     */
    @Test
    public void partitionsSchedules() throws Exception {
        Client client = getClient(0);

        String tableName = getTableName(0, TableType.PARTITIONED);

        String procName = getMethodName() + "_prune";
        client.callProcedure("@AdHoc",
                "CREATE PROCEDURE " + procName + " DIRECTED AS DELETE FROM " + tableName
                        + " ORDER BY key OFFSET 10");

        AtomicReference<Exception> error = new AtomicReference<>();
        Thread producer = new Thread() {
            @Override
            public void run() {
                try {
                    while (true) {
                        insertRandomRows(client, 0, TableType.PARTITIONED, 10);
                        Thread.sleep(1);
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    error.set(e);
                }
            };
        };
        producer.start();

        String schedule = getMethodName();
        client.callProcedure("@AdHoc",
                "CREATE TASK " + schedule + " ON SCHEDULE DELAY 10 MILLISECONDS PROCEDURE " + procName
                        + " RUN ON PARTITIONS");

        // Give everything some time to run
        Thread.sleep(1000);
        producer.interrupt();
        producer.join();

        VoltTable table = getTaskStats(client);
        assertEquals(6, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("RUNNING", table.getString("STATE"));
        }

        Thread.sleep(15);

        client.callProcedure("@AdHoc", "ALTER TASK " + schedule + " DISABLE;");

        Thread.sleep(5);

        table = getTaskStats(client);
        assertEquals(6, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("DISABLED", table.getString("STATE"));
            assertTrue("Scheduler invocation count is lower than expected: " + table.getLong("SCHEDULER_INVOCATIONS"),
                    table.getLong("SCHEDULER_INVOCATIONS") >= 50);
            assertTrue("Procedure invocation count is lower than expected: " + table.getLong("PROCEDURE_INVOCATIONS"),
                    table.getLong("PROCEDURE_INVOCATIONS") >= 50);
        }

        assertEquals(60,
                client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM " + tableName).getResults()[0].asScalarLong());

        // Test that partition schedules fail over
        getCluster(0).killSingleHost(1);

        // Give the system some time to complete the failover
        Thread.sleep(500);

        table = getTaskStats(client);
        assertEquals(6, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("DISABLED", table.getString("STATE"));
        }

        client.callProcedure("@AdHoc", "ALTER TASK " + schedule + " ENABLE;");

        table = getTaskStats(client);
        assertEquals(6, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("RUNNING", table.getString("STATE"));
        }

        try {
            client.callProcedure("@AdHoc", "DROP PROCEDURE " + procName);
            fail("Should not have been able to drop: " + procName);
        } catch (ProcCallException e) {
            String status = e.getClientResponse().getStatusString();
            assertTrue(status, status.contains("Invalid DROP PROCEDURE statement: " + procName));
        }
    }

    /*
     * Test that a custom scheduler can be provided in the DDL
     */
    @Test
    public void customScheduler() throws Exception {
        Client client = getClient(0);
        client.callProcedure("@AdHoc",
                "CREATE TASK " + getMethodName() + " FROM CLASS " + CustomScheduler.class.getName()
                        + " WITH (5, NULL);");
        Thread.sleep(1000);

        VoltTable table = getTaskStats(client);
        while (table.advanceRow()) {
            assertEquals("RUNNING", table.getString("STATE"));
            assertNull(table.getString("SCHEDULER_STATUS"));
        }

        client.callProcedure("@AdHoc", "DROP TASK " + getMethodName());
        client.callProcedure("@AdHoc",
                "CREATE TASK " + getMethodName() + " FROM CLASS " + CustomScheduler.class.getName()
                        + " WITH (5, 'STATUS');");

        table = getTaskStats(client);
        while (table.advanceRow()) {
            assertEquals("RUNNING", table.getString("STATE"));
            assertEquals("STATUS", table.getString("SCHEDULER_STATUS"));
        }
    }

    /*
     * Test creating tasks with AS USER clause
     */
    @Test
    public void tasksAsUser() throws Exception {
        Client client = getClient(0);

        String tableName = getTableName(0, TableType.PARTITIONED);
        String procName = getMethodName() + "_prune";

        client.callProcedure("@AdHoc",
                "CREATE PROCEDURE " + procName + " DIRECTED AS DELETE FROM " + tableName
                        + " ORDER BY key OFFSET 10");

        try {
            client.callProcedure("@AdHoc",
                    "CREATE TASK " + getMethodName() + " ON SCHEDULE DELAY 5 MILLISECONDS PROCEDURE " + procName
                            + " RUN ON PARTITIONS AS USER BOGUS_USER;");
            fail("Should have failed to create a task with a bogus user");
        } catch (ProcCallException e) {}

        client.callProcedure("@AdHoc", "CREATE TASK " + getMethodName() + " ON SCHEDULE DELAY 5 MILLISECONDS PROCEDURE "
                + procName + " RUN ON PARTITIONS AS USER " + s_userName + ";");

        Thread.sleep(500);

        VoltTable table = getTaskStats(client);
        assertEquals(6, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("RUNNING", table.getString("STATE"));
        }

        m_builder.clearUsers();
        try {
            getCluster(0).updateCatalog(m_builder);
            fail("Should have failed to update the catalog since the user is in use");
        } catch (ProcCallException e) {}
    }

    /*
     * Test that limitations are enforced in community and not in pro
     */
    @Test
    public void communityLimitations() throws Exception {
        Client client = getClient(0);

        // Test ability to use system procedures
        try {
            client.callProcedure("@AdHoc",
                    "CREATE TASK " + getMethodName()
                            + " ON SCHEDULE DELAY 5 SECONDS PROCEDURE @AdHoc WITH ('SELECT * FROM "
                            + getTableName(0, TableType.REPLICATED) + ";');");
            assertTrue(MiscUtils.isPro());
            client.callProcedure("@AdHoc", "DROP TASK " + getMethodName() + ';');
        } catch (ProcCallException e) {
            assertFalse(MiscUtils.isPro());
            String status = e.getClientResponse().getStatusString();
            assertTrue(status, status.contains("System procedures are not allowed in tasks"));
        }

        // Test ability to use custom action generator
        try {
            client.callProcedure("@AdHoc", "CREATE TASK " + getMethodName()
                    + " ON SCHEDULE DELAY 5 SECONDS PROCEDURE FROM CLASS " + CustomTestActionGenerator.class.getName()
                    + ';');
            assertTrue(MiscUtils.isPro());
            client.callProcedure("@AdHoc", "DROP TASK " + getMethodName() + ';');
        } catch (ProcCallException e) {
            assertFalse(MiscUtils.isPro());
            String status = e.getClientResponse().getStatusString();
            assertTrue(status, status.contains("Custom action generator class not allowed in tasks"));
        }

        // Test ability to use custom action scheduler
        try {
            client.callProcedure("@AdHoc", "CREATE TASK " + getMethodName()
                    + " FROM CLASS " + CustomTestActionScheduler.class.getName() + ';');
            assertTrue(MiscUtils.isPro());
            client.callProcedure("@AdHoc", "DROP TASK " + getMethodName() + ';');
        } catch (ProcCallException e) {
            assertFalse(MiscUtils.isPro());
            String status = e.getClientResponse().getStatusString();
            assertTrue(status, status.contains("Custom action scheduler class not allowed in tasks"));
        }
    }

    static VoltTable getTaskStats(Client client)
            throws NoConnectionsException, IOException, ProcCallException {
        return client.callProcedure("@Statistics", "TASK", 0).getResults()[0];
    }

    public static class CustomScheduler implements ActionScheduler {
        private int m_delayMs;
        private String m_status;

        public void initialize(int delayMs, String status) {
            m_delayMs = delayMs;
            m_status = status;
        }

        @Override
        public ScheduledAction getFirstScheduledAction() {
            return getNextAction(null);
        }

        public ScheduledAction getNextAction(ActionResult result) {
            return ScheduledAction.callback(m_delayMs, TimeUnit.MILLISECONDS, this::getNextAction)
                    .setStatusMessage(m_status);
        }

    }
}
