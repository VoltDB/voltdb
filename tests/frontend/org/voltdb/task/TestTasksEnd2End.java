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

public class TestTasksEnd2End extends LocalClustersTestBase {
    @Override
    public void setUp() throws Exception {
        super.setUp();

        configureClustersAndClients(Collections.singletonList(new ClusterConfiguration(4, 3, 1)), 2, 2);
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

        String summaryFormat = "CREATE TASK %s ON SCHEDULE %s PROCEDURE @AdHoc WITH ('INSERT INTO " + summaryTable
                + " SELECT NOW, %d, COUNT(*), SUM(CAST(key as DECIMAL)), SUM(CAST(value AS DECIMAL)) FROM "
                + getTableName(0, TableType.REPLICATED) + ";') ON ERROR IGNORE;";

        client.callProcedure("@AdHoc", String.format(summaryFormat, schedule1, "delay 50 MILLISECONDS", 1));
        client.callProcedure("@AdHoc", String.format(summaryFormat, schedule2, "CRON * * * * * *", 2));
        client.callProcedure("@AdHoc", String.format(summaryFormat, schedule3, "EVERY 75 milliseconds", 3));

        // Give everything some time to run
        Thread.sleep(1000);
        producer.interrupt();
        producer.join();

        VoltTable table = getScheduleStats(client);
        assertEquals(3, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("RUNNING", table.getString("STATE"));
        }

        client.callProcedure("@AdHoc", "ALTER TASK " + schedule1 + " DISABLE; ALTER TASK " + schedule2
                + " DISABLE; ALTER TASK " + schedule3 + " DISABLE;");

        Thread.sleep(5);

        table = getScheduleStats(client);
        assertEquals(3, table.getRowCount());
        while (table.advanceRow()) {
            String scheduleName = table.getString("NAME");
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
                "CREATE PROCEDURE " + procName + " PARTITIONED AS DELETE FROM " + tableName
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

        VoltTable table = getScheduleStats(client);
        assertEquals(6, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("RUNNING", table.getString("STATE"));
        }

        Thread.sleep(15);

        client.callProcedure("@AdHoc", "ALTER TASK " + schedule + " DISABLE;");

        Thread.sleep(5);

        table = getScheduleStats(client);
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

        table = getScheduleStats(client);
        assertEquals(4, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("DISABLED", table.getString("STATE"));
        }

        client.callProcedure("@AdHoc", "ALTER TASK " + schedule + " ENABLE;");

        table = getScheduleStats(client);
        assertEquals(6, table.getRowCount());
        while (table.advanceRow()) {
            assertEquals("RUNNING", table.getString("STATE"));
        }

        try {
            client.callProcedure("@AdHoc", "DROP PROCEDURE " + procName);
            fail("Should not have been able to drop: " + procName);
        } catch (ProcCallException e) {
            String status = e.getClientResponse().getStatusString();
            assertTrue(status, status.contains("Procedure does not exist: " + procName));
        }
    }

    @Test
    public void customScheduler() throws Exception {
        Client client = getClient(0);
        client.callProcedure("@AdHoc",
                "CREATE TASK " + getMethodName() + " FROM CLASS " + CustomScheduler.class.getName()
                        + " WITH (5, NULL);");
        Thread.sleep(1000);

        VoltTable table = getScheduleStats(client);
        while (table.advanceRow()) {
            assertEquals("RUNNING", table.getString("STATE"));
            assertNull(table.getString("SCHEDULER_STATUS"));
        }

        client.callProcedure("@AdHoc", "DROP TASK " + getMethodName());
        client.callProcedure("@AdHoc",
                "CREATE TASK " + getMethodName() + " FROM CLASS " + CustomScheduler.class.getName()
                        + " WITH (5, 'STATUS');");

        table = getScheduleStats(client);
        while (table.advanceRow()) {
            assertEquals("RUNNING", table.getString("STATE"));
            assertEquals("STATUS", table.getString("SCHEDULER_STATUS"));
        }

    }

    private static VoltTable getScheduleStats(Client client)
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
        public DelayedAction getFirstDelayedAction() {
            return getNextAction(null);
        }

        public DelayedAction getNextAction(ActionResult result) {
            return DelayedAction.createCallback(m_delayMs, TimeUnit.MILLISECONDS, this::getNextAction)
                    .setStatusMessage(m_status);
        }

    }
}
